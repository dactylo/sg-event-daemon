"""Durable generation and deadline-driven worker supervision."""

from __future__ import annotations

import logging
import os
import resource
import socket
import sqlite3
import sys
import time
import uuid
from collections import deque
from contextlib import ExitStack, closing, contextmanager, suppress
from dataclasses import dataclass, field, replace
from math import isfinite
from typing import TYPE_CHECKING, Literal, NotRequired, Protocol, TypedDict, Unpack, cast

from . import _control, _source_store, _write_lock
from ._checkpoint import Checkpointer
from ._config import (
    ConfigError,
    WorkerConfig,
    group_state_options,
    source_state_options,
    source_stream_fingerprint,
)
from ._control import (
    GROUP_BUSY_CODE,
    GROUP_DISPOSITION_ARGUMENTS,
    SOURCE_ADOPTION_ARGUMENTS,
    SOURCE_BUSY_CODE,
    SOURCE_DATABASE_BUSY_CODE,
    SUPERVISOR_DATABASE_BUSY_CODE,
    encode_json_body,
)
from ._group_errors import InvalidTransitionError
from ._group_recovery import (
    DispositionRequest,
    LifecycleRequest,
    control_peer_label,
    disposition_admitted,
    disposition_command,
    group_result_mapping,
)
from ._group_state import GroupState
from ._group_types import (
    AuditIdentity,
    ConditionalParkDirective,
    StartMode,
    is_start_mode,
)
from ._health import (
    HEALTH_TEXT_MAX_BYTES,
    BreakerTransition,
    GroupHealth,
    WatchdogHealth,
    aggregate_status,
    evaluate_health,
    looks_secret,
)
from ._ipc import (
    OUTCOME_KEY,
    OUTCOME_UNKNOWN,
    ActiveSourceGroupAssignment,
    CommandResult,
    ControlHello,
    ControlMessage,
    DeliveryGroupAssignment,
    EventPeerGrant,
    EventPeerReady,
    GroupRecoveryReady,
    HealthUpdate,
    OperatorCommand,
    PendingDispositionSourceGroupAssignment,
    SourceEndpointReady,
    SourceGroupAssignment,
    StopCommand,
    WorkerAssignment,
    WorkerControl,
)
from ._platform import (
    AlreadyLockedError,
    ChildProcess,
    FileLock,
    SecurityError,
    acquire_lock,
    boot_identity,
    ensure_secure_state_dir,
    local_socket_path_candidate,
    raise_open_file_limit,
    reap_child,
    release_flock,
    secure_open_regular_file,
    spawn_child,
    terminate_child,
    try_flock_exclusive,
)
from ._probe import ProbeResult, ProbeSnapshot, SupervisorProgress
from ._resources import close_callables, close_resources
from ._source_state import SourceState
from ._source_store import SourceIdentityError
from ._sqlite import (
    BUSY_TIMEOUT_SECS,
    DatabaseError,
    DatabaseHandle,
    Schema,
    database_family_size,
    open_database,
    temporary_busy_timeout,
    transaction,
)
from ._state_paths import (
    group_lock_path,
    site_groups_directory,
    site_sources_directory,
    source_database_path,
    source_lock_path,
    source_socket_kind,
    source_write_lock_path,
)
from ._watchdog import internal_stall_deadline_secs, watchdog_grace_secs
from ._worker import (
    OPEN_FILE_MARGIN,
    WorkerSpec,
    build_worker_specs,
    open_file_need,
    stable_operator_id,
)
from ._write_lock import (
    OPERATOR_WRITE_BUDGET_SECS,
    SourceDatabaseBusyError,
    SourceWriteLock,
    WriteContext,
    WriteLockStoppedError,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Generator, Iterator, Mapping
    from pathlib import Path
    from types import TracebackType
    from typing import Self

    from ._config import Config, SourceConfig
    from ._group_types import ReplayRequiredNotice
    from ._source import SourceHealth
    from ._watchdog import GroupPhase, SourceWatchdogPhase

_STARTUP_WRITE_WARNING_SECS = 5.0

_SECONDS_PER_MINUTE = 60.0

_WORKER_BREAKER_HISTORY_LIMIT = 100

_BOOT_TIMING_STATEMENT = """
CREATE TABLE supervisor_timing (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    boot_identity TEXT NOT NULL
)
"""
_BREAKER_AUDIT_STATEMENT = """
CREATE TABLE breaker_audit (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id TEXT NOT NULL,
    prior_revision INTEGER NOT NULL,
    new_revision INTEGER NOT NULL,
    command TEXT NOT NULL,
    reason TEXT NOT NULL,
    wall_clock_time REAL NOT NULL,
    local_uid INTEGER NOT NULL,
    username TEXT NOT NULL,
    pid INTEGER NOT NULL
)
"""
_LOGGER = logging.getLogger('sg_event_daemon.supervisor')
_PARK_EVIDENCE_STATEMENT = """
CREATE TABLE park_evidence (
    evidence_id TEXT PRIMARY KEY,
    site TEXT NOT NULL,
    group_name TEXT NOT NULL,
    group_generation INTEGER NOT NULL,
    failed_worker_id TEXT NOT NULL,
    failed_spawn_instance TEXT NOT NULL,
    event_id INTEGER NOT NULL,
    group_revision INTEGER NOT NULL,
    observed_at_wall_clock REAL NOT NULL,
    UNIQUE (site, group_name)
)
"""
_STATUS_RESULT_MAX_BYTES = 786_432

# Reconciliation census: 3 stdio, 7 with no authorities, 14 with one source.
_SUPERVISOR_FIXED_DESCRIPTORS = 7
_SUPERVISOR_SOURCE_DESCRIPTORS = 7

_SUPERVISOR_SCHEMA = Schema(
    version=1,
    statements=(
        """
        CREATE TABLE group_inventory (
            site TEXT NOT NULL,
            group_name TEXT NOT NULL,
            source TEXT NOT NULL,
            group_generation INTEGER NOT NULL,
            orphan_generation INTEGER,
            start_mode TEXT NOT NULL
                CHECK (start_mode IN ('initialize', 'resume')),
            configured INTEGER NOT NULL CHECK (configured IN (0, 1)),
            lifecycle TEXT NOT NULL
                CHECK (lifecycle IN ('active', 'orphaned', 'readded-parked')),
            PRIMARY KEY (site, group_name)
        )
        """,
        """
        CREATE TABLE generation_high_water (
            site TEXT NOT NULL,
            group_name TEXT NOT NULL,
            group_generation INTEGER NOT NULL,
            PRIMARY KEY (site, group_name)
        )
        """,
        _BREAKER_AUDIT_STATEMENT,
        _PARK_EVIDENCE_STATEMENT,
        """
        CREATE TABLE worker_breaker (
            worker_id TEXT PRIMARY KEY,
            restart_limit INTEGER NOT NULL,
            restart_window_secs REAL NOT NULL,
            healthy_reset_secs REAL NOT NULL,
            is_open INTEGER NOT NULL CHECK (is_open IN (0, 1)),
            revision INTEGER NOT NULL,
            last_failure_at REAL,
            restart_deadline_at REAL,
            failure_count INTEGER NOT NULL,
            transition_count INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE worker_breaker_failures (
            worker_id TEXT NOT NULL,
            failure_index INTEGER NOT NULL,
            failure_at REAL NOT NULL,
            PRIMARY KEY (worker_id, failure_index),
            FOREIGN KEY (worker_id) REFERENCES worker_breaker (worker_id)
                ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE worker_breaker_transitions (
            worker_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('closed', 'open')),
            reason TEXT NOT NULL,
            PRIMARY KEY (worker_id, sequence),
            FOREIGN KEY (worker_id) REFERENCES worker_breaker (worker_id)
                ON DELETE CASCADE
        )
        """,
        _BOOT_TIMING_STATEMENT,
    ),
)
_WORKER_STALE_INTERVALS = 5


class OperatorOutcomeUnknownError(RuntimeError):
    """Report a dispatched command whose final outcome is not known."""


class _FailedWorkerCleanupOptions(TypedDict):
    """Type one bounded failed child cleanup request."""

    process: ChildProcess
    shutdown_deadline_secs: float
    wait_slice_secs: float
    foreground_progress: Callable[[], None] | None
    spec: WorkerSpec
    spawn_instance: str


@dataclass(frozen=True)
class _HealthInventory:
    """Hold one validated current-spawn component inventory."""

    source_records: dict[str, SourceHealth]
    group_records: dict[str, GroupHealth]
    source_watchdogs: dict[str, WatchdogHealth]
    lane_watchdogs: dict[str, WatchdogHealth]
    operator_count: int


@dataclass(frozen=True)
class _InventoryRecord:
    """Project one durable configured-group inventory row."""

    configured: bool
    generation: int
    lifecycle: Literal['active', 'orphaned', 'readded-parked']
    orphan_generation: int | None
    source: str
    start_mode: StartMode

    def parked_orphan_generation(self) -> int:
        """Validate the parked reservation fence and return its orphan generation.

        Raises:
            RuntimeError: The orphan generation is absent or is not below the
                reserved generation.
        """
        orphan_generation = self.orphan_generation
        if orphan_generation is None or self.generation <= orphan_generation:
            raise RuntimeError('parked inventory generation fence is invalid')
        return orphan_generation


@dataclass(frozen=True)
class _ParkEvidence:
    """Associate one validated park directive with its failed worker."""

    directive: ConditionalParkDirective
    failed_worker_id: str

    def __post_init__(self) -> None:
        """Reject an empty worker identity."""
        if not self.failed_worker_id:
            raise ValueError('park evidence worker identity must not be empty')


class _PendingWrites:
    """Keep coalesced projections and identity-keyed transitions in arrival order."""

    def __init__(self) -> None:
        """Create an empty process-local write collection."""
        self.writes: dict[tuple[str | int, ...], Callable[[sqlite3.Connection], None]] = {}
        self.parks: dict[str, _ParkEvidence] = {}

    def apply(self, connection: sqlite3.Connection) -> None:
        """Apply oldest first without discarding anything before commit.

        Args:
            connection: Live transaction connection; the caller owns commit or
                rollback.

        Raises:
            Exception: A queued write fails; no entries are discarded by this
                method.
        """
        for write in self.writes.values():
            write(connection)

    def clear(self) -> None:
        """Discard the current batch after the caller commits it successfully."""
        self.writes.clear()
        self.parks.clear()


class _UnreapedWorkerSpawnError(RuntimeError):
    """Expose explicit ownership of a child that construction could not reap."""

    def __init__(
        self,
        *,
        process: ChildProcess,
        spec: WorkerSpec,
        spawn_instance: str,
    ) -> None:
        """Retain the exact child handle and failed spawn identity."""
        super().__init__(
            f'worker construction failed and child remained unreaped: {spec.worker_id}'
        )
        self._process = process
        self._spawn_instance = spawn_instance
        self._spec = spec


@dataclass(frozen=True)
class _InventoryReconciliation:
    """Carry one complete configured and durable group observation."""

    group: str
    group_generation: int | None
    previous: _InventoryRecord | None
    site: str
    source: str


@dataclass(frozen=True)
class _OrphanGroupMetadata:
    """Project one absent orphan's durable group identity."""

    group_generation: int
    ordering: Literal['eager', 'strict']
    site: str
    source: str
    source_generation: str
    verified_incarnation: str


@dataclass(frozen=True)
class _ReconciliationSourceMetadata:
    """Project one durable source identity."""

    source_generation: str


@dataclass(frozen=True)
class _WorkerBreakerProjection:
    """Carry one complete durable worker-breaker projection."""

    worker_id: str
    restart_limit: int
    restart_window_secs: float
    healthy_reset_secs: float
    is_open: bool
    revision: int
    last_failure_at: float | None
    restart_deadline_at: float | None
    failure_count: int
    failure_times: tuple[float, ...]
    transition_count: int
    history: tuple[BreakerTransition, ...]


class _WorkerBreaker:
    """Track one worker's independent restart window and circuit state."""

    def __init__(
        self,
        *,
        worker_id: str,
        restart_limit: int,
        restart_window_secs: float,
        healthy_reset_secs: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Create one closed per-worker breaker."""
        if not worker_id:
            raise ValueError('worker breaker identity must not be empty')
        if type(restart_limit) is not int or restart_limit < 0:
            raise ValueError('worker restart limit must be nonnegative')
        if (
            not isfinite(restart_window_secs)
            or restart_window_secs <= 0
            or not isfinite(healthy_reset_secs)
            or healthy_reset_secs <= 0
        ):
            raise ValueError('worker breaker intervals must be finite and positive')
        self._clock = clock
        self._failure_total = 0
        self._failure_times: deque[float] = deque()
        self._healthy_reset_secs = healthy_reset_secs
        self._failure_history: deque[BreakerTransition] = deque(
            maxlen=_WORKER_BREAKER_HISTORY_LIMIT
        )
        self._breaker_open = False
        self._last_failure_at: float | None = None
        self._restart_deadline: float | None = None
        self._restart_limit = restart_limit
        self._restart_window_secs = restart_window_secs
        self._breaker_revision = 0
        self._transition_total = 0
        self.worker_id = worker_id

    def _append_transition(
        self,
        state: Literal['closed', 'open'],
        reason: str,
    ) -> BreakerTransition:
        """Append one bounded transition and cumulative sequence."""
        self._transition_total += 1
        transition = BreakerTransition(
            sequence=self._transition_total,
            worker_id=self.worker_id,
            state=state,
            reason=reason,
        )
        self._failure_history.append(transition)
        return transition

    @classmethod
    def from_projection(
        cls,
        projection: _WorkerBreakerProjection,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> Self:
        """Restore one validated complete durable projection.

        Args:
            projection: Persisted breaker configuration, counters, timing, and
                transition history.
            clock: Supplier of monotonic seconds for subsequent transitions.

        Raises:
            RuntimeError: Persisted counters, timing, or transition history
                conflict with the breaker state.
            ValueError: The worker identity is empty, the restart limit is not
                an exact nonnegative integer, or either configured interval is
                not finite and positive.
        """
        failure_times = projection.failure_times
        history = projection.history
        if (
            type(projection.failure_count) is not int
            or projection.failure_count < len(failure_times)
            or type(projection.revision) is not int
            or projection.revision < 0
            or type(projection.transition_count) is not int
            or projection.transition_count < len(history)
            or any(not isfinite(failure_at) or failure_at < 0 for failure_at in failure_times)
            or tuple(sorted(failure_times)) != failure_times
            or (
                projection.last_failure_at is not None
                and (not isfinite(projection.last_failure_at) or projection.last_failure_at < 0)
            )
            or (
                projection.restart_deadline_at is not None
                and (
                    not isfinite(projection.restart_deadline_at)
                    or projection.restart_deadline_at < 0
                )
            )
            or (projection.is_open and projection.restart_deadline_at is not None)
            or any(transition.worker_id != projection.worker_id for transition in history)
            or any(
                history[index - 1].sequence >= history[index].sequence
                for index in range(1, len(history))
            )
            or (projection.is_open and not history)
            or (
                history
                and (
                    history[-1].sequence > projection.transition_count
                    or history[-1].state != ('open' if projection.is_open else 'closed')
                )
            )
        ):
            raise RuntimeError('durable worker breaker projection is invalid')
        breaker = cls(
            worker_id=projection.worker_id,
            restart_limit=projection.restart_limit,
            restart_window_secs=projection.restart_window_secs,
            healthy_reset_secs=projection.healthy_reset_secs,
            clock=clock,
        )
        breaker._failure_total = projection.failure_count
        breaker._failure_times.extend(failure_times)
        breaker._failure_history.extend(history)
        breaker._breaker_open = projection.is_open
        breaker._last_failure_at = projection.last_failure_at
        breaker._restart_deadline = projection.restart_deadline_at
        breaker._breaker_revision = projection.revision
        breaker._transition_total = projection.transition_count
        return breaker

    @property
    def history(self) -> tuple[BreakerTransition, ...]:
        """Return the retained open and reset transitions.

        Returns:
            Up to the latest 100 retained open/reset transitions in increasing
            sequence order.
        """
        return tuple(self._failure_history)

    @property
    def is_open(self) -> bool:
        """Return whether automatic restart is disabled."""
        return self._breaker_open

    def mark_restart_started(self) -> bool:
        """Clear a consumed durable restart deadline.

        Returns:
            `True` if a deadline was cleared, or `False` if none was pending.
        """
        if self._restart_deadline is None:
            return False
        self._restart_deadline = None
        return True

    @property
    def projection(self) -> _WorkerBreakerProjection:
        """Return one complete immutable persistence projection."""
        return _WorkerBreakerProjection(
            worker_id=self.worker_id,
            restart_limit=self._restart_limit,
            restart_window_secs=self._restart_window_secs,
            healthy_reset_secs=self._healthy_reset_secs,
            is_open=self._breaker_open,
            revision=self._breaker_revision,
            last_failure_at=self._last_failure_at,
            restart_deadline_at=self._restart_deadline,
            failure_count=self._failure_total,
            failure_times=tuple(self._failure_times),
            transition_count=self._transition_total,
            history=tuple(self._failure_history),
        )

    def record_failure(self, reason: str) -> float | None:
        """Record an unattributed failure and return its restart delay.

        Returns:
            Restart delay in seconds, doubling from one second to a maximum of
            60 seconds within the failure window. Returns `None` when the
            breaker is already open or this failure exceeds its restart limit.

        Raises:
            ValueError: The reason is empty or the clock value is negative or
                non-finite.
        """
        if not reason:
            raise ValueError('worker failure reason must not be empty')
        now = self._clock()
        if not isfinite(now) or now < 0:
            raise ValueError('worker breaker clock must be finite and nonnegative')
        if self._breaker_open:
            return None
        while self._failure_times and now - self._failure_times[0] > self._restart_window_secs:
            self._failure_times.popleft()
        self._failure_total += 1
        self._failure_times.append(now)
        self._last_failure_at = now
        if len(self._failure_times) > self._restart_limit:
            self._breaker_open = True
            self._restart_deadline = None
            self._breaker_revision += 1
            self._append_transition('open', reason)
            return None
        exponent = min(len(self._failure_times) - 1, 6)
        delay = min(60.0, 2.0**exponent)
        self._restart_deadline = now + delay
        return delay

    def record_healthy(self) -> bool:
        """Clear closed-breaker backoff after sustained health.

        Returns:
            `True` when the healthy-reset interval has elapsed since the last
            failure and backoff is cleared; otherwise `False`.
        """
        if self._breaker_open or self._last_failure_at is None:
            return False
        now = self._clock()
        if now - self._last_failure_at < self._healthy_reset_secs:
            return False
        self._failure_times.clear()
        self._last_failure_at = None
        self._restart_deadline = None
        return True

    @property
    def restart_deadline_at(self) -> float | None:
        """Return the durable monotonic restart deadline.

        Returns:
            Absolute monotonic restart time, or `None` when no automatic
            restart is scheduled.
        """
        return self._restart_deadline

    @property
    def revision(self) -> int:
        """Return the compare-and-swap breaker revision."""
        return self._breaker_revision

    @property
    def transition_count(self) -> int:
        """Return the cumulative unbounded transition count."""
        return self._transition_total

    def validate_reset(self, *, expected_revision: int, reason: str) -> None:
        """Validate one breaker reset without mutating state.

        Raises:
            ValueError: The reason is empty or exceeds 4096 UTF-8 bytes, the
                expected revision conflicts, or the breaker is not open.
        """
        if not reason:
            raise ValueError('worker breaker reset reason must not be empty')
        if len(reason.encode()) > HEALTH_TEXT_MAX_BYTES:
            raise ValueError('worker breaker reset reason exceeds 4096 UTF-8 bytes')
        if expected_revision != self._breaker_revision:
            raise ValueError('worker breaker reset revision conflicts')
        if not self._breaker_open:
            raise ValueError('worker breaker is not open')

    def apply_audited_reset(
        self,
        *,
        expected_revision: int,
        reason: str,
    ) -> BreakerTransition:
        """Apply one already-audited compare-and-swap reset.

        Call only after the matching durable reset audit commits. This clears
        failure backoff, advances the revision, and appends a closed
        transition.

        Raises:
            ValueError: The reason is empty or exceeds 4096 UTF-8 bytes, the
                expected revision conflicts, or the breaker is not open.
        """
        self.validate_reset(expected_revision=expected_revision, reason=reason)
        self._breaker_open = False
        self._failure_times.clear()
        self._last_failure_at = None
        self._restart_deadline = None
        self._breaker_revision += 1
        return self._append_transition('closed', reason)


class _SupervisorCheckpointOptions(TypedDict, total=False):
    """Carry checkpoint timing and constructor injection to durable state."""

    interval_secs: float
    clock: Callable[[], float]
    checkpointer_factory: Callable[..., Checkpointer]


class _WorkerBreakerRestoreOptions(TypedDict):
    """Type one complete durable breaker restore request."""

    worker_id: str
    restart_limit: int
    restart_window_secs: float
    healthy_reset_secs: float
    clock: NotRequired[Callable[[], float]]
    boot_id: NotRequired[str | None]


class _SupervisorDatabaseBusyError(_control.ControlCommandError):
    """Reject an operator write that could not acquire its durable fence."""

    def __init__(self, message: str) -> None:
        """Preserve the supervisor busy code through the control response."""
        super().__init__(message, result={'code': SUPERVISOR_DATABASE_BUSY_CODE})


class _SupervisorState:
    """Own durable configured inventory and generation high-water marks."""

    def __init__(
        self,
        database: DatabaseHandle,
        *,
        interval_secs: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
        checkpointer_factory: Callable[..., Checkpointer] = Checkpointer,
    ) -> None:
        """Store one open supervisor database handle."""
        self._database = database
        self._clock = clock
        self.pending = _PendingWrites()
        self._write_stalled_at: float | None = None
        self._startup_stop: Callable[[], bool] | None = None
        self._checkpointer = checkpointer_factory(
            database.connection,
            database.path,
            interval_secs=interval_secs,
            clock=clock,
            logger=_LOGGER,
            log_extra={},
        )

    def __enter__(self) -> Self:
        """Return this open durable supervisor state."""
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Close the durable supervisor state."""
        self.close()

    def _clear_write_stall(self) -> None:
        """Publish the end of one pending-write episode."""
        if self._write_stalled_at is not None:
            _LOGGER.info(
                'supervisor.db pending writes stored after %.1fs',
                self._clock() - self._write_stalled_at,
            )
            self._write_stalled_at = None

    @contextmanager
    def _write_transaction(self, timeout_millis: int) -> Generator[sqlite3.Connection]:
        """Change the busy timeout only for acquisition, restoring it on every exit."""
        connection = self._database.connection
        with ExitStack() as stack:
            with temporary_busy_timeout(connection, timeout_millis):
                stack.enter_context(transaction(connection))
            yield connection

    @contextmanager
    def _startup_transaction(
        self,
        should_stop: Callable[[], bool],
    ) -> Generator[sqlite3.Connection]:
        """Retry startup acquisition in one-second, stop-aware slices."""
        started = self._clock()
        warned = False
        waited = False
        with ExitStack() as stack:
            while True:
                if should_stop():
                    raise WriteLockStoppedError('supervisor database startup stopped')
                try:
                    connection = stack.enter_context(self._write_transaction(1000))
                    break
                except DatabaseError as error:
                    if not _supervisor_write_busy(error):
                        raise
                    waited = True
                    if not warned and self._clock() - started >= _STARTUP_WRITE_WARNING_SECS:
                        _LOGGER.warning(
                            'supervisor.db is held by a connection outside the daemon; '
                            'startup is waiting'
                        )
                        warned = True
            if waited:
                _LOGGER.info(
                    'supervisor.db write began after waiting %.1fs', self._clock() - started
                )
            yield connection

    def _normalize_breaker_timing(self, boot_id: str, now: float) -> None:
        """Normalize every absolute monotonic projection after a boot change."""
        if not boot_id:
            raise ValueError('supervisor boot identity must not be empty')
        if not isfinite(now) or now < 0:
            raise ValueError('worker breaker clock must be finite and nonnegative')
        row = self._database.connection.execute(
            'SELECT boot_identity FROM supervisor_timing WHERE singleton = 1'
        ).fetchone()
        if row is not None and str(row[0]) == boot_id:
            return
        with self._startup_transaction(self._startup_stop or (lambda: False)) as connection:
            connection.execute(
                """
                UPDATE worker_breaker
                SET last_failure_at = CASE
                        WHEN last_failure_at IS NULL THEN NULL
                        ELSE ?
                    END,
                    restart_deadline_at = CASE
                        WHEN restart_deadline_at IS NULL THEN NULL
                        ELSE ?
                    END
                """,
                (now, now),
            )
            connection.execute(
                'UPDATE worker_breaker_failures SET failure_at = ?',
                (now,),
            )
            connection.execute(
                """
                INSERT INTO supervisor_timing (singleton, boot_identity)
                VALUES (1, ?)
                ON CONFLICT (singleton) DO UPDATE SET
                    boot_identity = excluded.boot_identity
                """,
                (boot_id,),
            )

    def audit_count(self) -> int:
        """Return the cumulative supervisor audit count.

        Raises:
            sqlite3.Error: Reading the supervisor audit count fails.
        """
        row = self._database.connection.execute('SELECT COUNT(*) FROM breaker_audit').fetchone()
        if row is None:
            raise RuntimeError('supervisor audit count is unavailable')
        return int(row[0])

    def checkpoint(self) -> None:
        """Checkpoint the supervisor database and report crossed runtime bounds.

        Raises:
            DatabaseError: A SQLite checkpoint operation fails.
            OSError: The WAL file cannot be inspected or synchronized.
        """
        self._checkpointer.run_once()

    def close(self) -> None:
        """Close the supervisor database.

        Raises:
            OSError: Closing the owned WAL sync descriptor fails.
        """
        close_resources((self._checkpointer, self._database))

    def conditions(self) -> tuple[str, ...]:
        """Compose independent checkpoint and pending-write readiness conditions."""
        return (
            *(('checkpoint-stalled',) if self._checkpointer.crossed else ()),
            *(('write-lock-stalled',) if self._write_stalled_at is not None else ()),
        )

    def configured_group_assignments(
        self,
    ) -> dict[tuple[str, str], SourceGroupAssignment]:
        """Project current durable configured inventory into closed assignments.

        Returns:
            Assignments keyed by `(site, group)`, including active and
            pending-disposition configured groups.

        Raises:
            sqlite3.Error: Reading durable inventory fails.
            RuntimeError: Stored inventory lifecycle, start mode, or
                orphan-generation fences are inconsistent.
            ValueError: Stored assignment identities or generations are
                invalid.
        """
        assignments: dict[tuple[str, str], SourceGroupAssignment] = {}
        for identity, record in _inventory_records(self._database.connection).items():
            if not record.configured:
                continue
            if record.lifecycle == 'active':
                if record.orphan_generation is not None:
                    raise RuntimeError('active inventory retains an orphan generation')
                assignment: SourceGroupAssignment = ActiveSourceGroupAssignment(
                    source=record.source,
                    group=identity[1],
                    group_generation=record.generation,
                    start_mode=record.start_mode,
                )
            elif record.lifecycle == 'readded-parked':
                orphan_generation = record.parked_orphan_generation()
                assignment = PendingDispositionSourceGroupAssignment(
                    source=record.source,
                    group=identity[1],
                    orphan_generation=orphan_generation,
                    reserved_generation=record.generation,
                )
            else:
                raise RuntimeError('configured inventory lifecycle is invalid')
            assignments[identity] = assignment
        return assignments

    def database_size_bytes(self) -> int:
        """Return current bytes used by the supervisor SQLite family.

        Raises:
            OSError: An existing database-family file cannot be inspected.
        """
        return database_family_size(self._database.path)

    def flush_pending(self) -> bool:
        """Commit the entire pending batch without waiting; return false only on busy.

        Raises:
            DatabaseError: SQLite fails for a reason other than retryable write
                contention.
            Exception: A queued transition fails; the pending batch is retained
                for the caller to resolve.
        """
        if not self.pending.writes:
            self._clear_write_stall()
            return True
        try:
            with self._write_transaction(0) as connection:
                self.pending.apply(connection)
        except DatabaseError as error:
            if not _supervisor_write_busy(error):
                raise
            if self._write_stalled_at is None:
                self._write_stalled_at = self._clock()
                _LOGGER.warning(
                    'supervisor.db writes are pending: a connection outside the daemon '
                    'holds the database'
                )
            return False
        self.pending.clear()
        self._clear_write_stall()
        return True

    def _queue_write(
        self,
        key: tuple[str | int, ...],
        write: Callable[[sqlite3.Connection], None],
    ) -> None:
        """Commit startup writes or queue and immediately try each runtime transition."""
        if self._startup_stop is not None:
            with self._startup_transaction(self._startup_stop) as connection:
                write(connection)
            return
        self.pending.writes[key] = write
        self.flush_pending()

    def complete_orphan_disposition(
        self,
        *,
        site: str,
        group: str,
        orphan_generation: int,
        reserved_generation: int,
    ) -> None:
        """Retain one terminal absent group at its disposed generation.

        Within `starting()`, commit before returning. Otherwise attempt the
        complete pending batch without waiting; contention leaves this change
        queued for `flush_pending()`.

        Args:
            site: Site identity.
            group: Group identity.
            orphan_generation: Retired generation expected by the terminal
                disposition.
            reserved_generation: Replacement generation already reserved in
                durable inventory.

        Raises:
            DatabaseError: SQLite fails or startup write acquisition is
                stopped.
            RuntimeError: The terminal orphan disposition conflicts with
                inventory.
            Exception: Another queued transition fails while the batch is being
                applied.
        """

        def store(connection: sqlite3.Connection) -> None:
            updated = connection.execute(
                """
                UPDATE group_inventory
                SET orphan_generation = NULL,
                    start_mode = 'resume'
                WHERE site = ? AND group_name = ?
                  AND configured = 0
                  AND lifecycle = 'orphaned'
                  AND group_generation = ?
                  AND orphan_generation = ?
                """,
                (
                    site,
                    group,
                    reserved_generation,
                    orphan_generation,
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError('terminal orphan disposition inventory conflicts')

        self._queue_write(('completion', site, group, reserved_generation), store)

    def generation_high_water(self, site: str, group: str) -> int:
        """Read the durable transition identity retained across pending removals.

        Raises:
            sqlite3.Error: Reading the durable generation fence fails.
            RuntimeError: No high-water row exists for the group.
        """
        row = self._database.connection.execute(
            'SELECT group_generation FROM generation_high_water WHERE site = ? AND group_name = ?',
            (site, group),
        ).fetchone()
        if row is None:
            raise RuntimeError('group generation high-water is unavailable')
        return int(row[0])

    def group_inventory_record(
        self,
        *,
        site: str,
        group: str,
    ) -> _InventoryRecord | None:
        """Return one exact durable inventory row.

        Returns:
            The validated row for `(site, group)`, or `None` when absent.

        Raises:
            sqlite3.Error: Reading durable inventory fails.
            RuntimeError: A stored inventory lifecycle or start mode is
                invalid.
        """
        return _inventory_records(self._database.connection).get((site, group))

    def complete_group_disposition(
        self,
        *,
        site: str,
        group: str,
        orphan_generation: int,
        reserved_generation: int,
    ) -> ActiveSourceGroupAssignment:
        """Promote one worker-confirmed terminal disposition generation.

        Within `starting()`, commit before returning. Otherwise attempt the
        complete pending batch without waiting; contention leaves this change
        queued for `flush_pending()`.

        Args:
            site: Site identity.
            group: Group identity.
            orphan_generation: Retired generation expected by the terminal
                disposition.
            reserved_generation: Replacement generation already reserved in
                durable inventory.

        Returns:
            The active resume assignment for the reserved generation, even if
            its inventory promotion remains queued.

        Raises:
            DatabaseError: SQLite fails or startup write acquisition is
                stopped.
            sqlite3.Error: Reading durable inventory fails.
            RuntimeError: The target inventory is absent, invalid, or conflicts
                with terminal disposition.
            ValueError: The resulting assignment identity or generation is
                invalid.
            Exception: Another queued transition fails while the batch is being
                applied.
        """
        row = self.group_inventory_record(site=site, group=group)
        if row is None:
            raise RuntimeError('terminal group disposition inventory is unavailable')

        def store(connection: sqlite3.Connection) -> None:
            if (
                _promote_readded_inventory(
                    connection,
                    site=site,
                    group=group,
                    reserved_generation=reserved_generation,
                    orphan_generation=orphan_generation,
                )
                != 1
            ):
                raise RuntimeError('terminal group disposition inventory conflicts')

        self._queue_write(('completion', site, group, reserved_generation), store)
        return ActiveSourceGroupAssignment(
            source=row.source,
            group=group,
            group_generation=reserved_generation,
            start_mode='resume',
        )

    def inventory_records(self) -> dict[tuple[str, str], _InventoryRecord]:
        """Return every validated durable group inventory row.

        Returns:
            Validated records keyed by `(site, group)`.

        Raises:
            sqlite3.Error: Reading durable inventory fails.
            RuntimeError: A stored inventory lifecycle or start mode is
                invalid.
        """
        return _inventory_records(self._database.connection)

    def orphaned_groups(self) -> dict[tuple[str, str], int]:
        """Return every omitted group and its retired generation.

        Returns:
            Omitted orphaned groups keyed by `(site, group)`, with their
            retained generations.

        Raises:
            sqlite3.Error: Reading durable inventory fails.
        """
        rows = self._database.connection.execute(
            """
            SELECT site, group_name, group_generation
            FROM group_inventory
            WHERE configured = 0 AND lifecycle = 'orphaned'
            ORDER BY site, group_name
            """
        ).fetchall()
        return {(str(row[0]), str(row[1])): int(row[2]) for row in rows}

    def pending_park_evidence(self) -> tuple[_ParkEvidence, ...]:
        """Return every durable pre-termination lane observation.

        Returns:
            Retained observations ordered by site and group.

        Raises:
            sqlite3.Error: Reading durable park evidence fails.
            ValueError: A stored observation has invalid identities,
                generations, event IDs, revisions, or observation time.
        """
        rows = self._database.connection.execute(
            """
            SELECT evidence_id, site, group_name, group_generation,
                   failed_worker_id, failed_spawn_instance, event_id,
                   group_revision, observed_at_wall_clock
            FROM park_evidence
            ORDER BY site, group_name
            """
        ).fetchall()
        return tuple(
            _ParkEvidence(
                directive=ConditionalParkDirective(
                    evidence_id=str(row[0]),
                    site=str(row[1]),
                    group=str(row[2]),
                    group_generation=int(row[3]),
                    failed_spawn_instance=str(row[5]),
                    event_id=int(row[6]),
                    group_revision=int(row[7]),
                    observed_at_wall_clock=float(row[8]),
                ),
                failed_worker_id=str(row[4]),
            )
            for row in rows
        )

    def persist_worker_breaker(self, breaker: _WorkerBreaker) -> None:
        """Replace one complete durable breaker projection atomically.

        Within `starting()`, commit before returning. Otherwise attempt the
        complete pending batch without waiting; contention leaves this change
        queued for `flush_pending()`.

        Args:
            breaker: Current complete in-memory breaker projection to retain.

        Raises:
            DatabaseError: SQLite fails or startup write acquisition is
                stopped.
            RuntimeError: The current operating-system boot identity cannot be
                obtained when initializing timing metadata.
            Exception: Another queued transition fails while the batch is being
                applied.
        """
        projection = breaker.projection

        def store(connection: sqlite3.Connection) -> None:
            timing = connection.execute(
                'SELECT boot_identity FROM supervisor_timing WHERE singleton = 1'
            ).fetchone()
            if timing is None:
                connection.execute(
                    """
                    INSERT INTO supervisor_timing (singleton, boot_identity)
                    VALUES (1, ?)
                    """,
                    (boot_identity(),),
                )
            connection.execute(
                """
                INSERT INTO worker_breaker (
                    worker_id, restart_limit, restart_window_secs,
                    healthy_reset_secs, is_open, revision, last_failure_at,
                    restart_deadline_at, failure_count, transition_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (worker_id) DO UPDATE SET
                    restart_limit = excluded.restart_limit,
                    restart_window_secs = excluded.restart_window_secs,
                    healthy_reset_secs = excluded.healthy_reset_secs,
                    is_open = excluded.is_open,
                    revision = excluded.revision,
                    last_failure_at = excluded.last_failure_at,
                    restart_deadline_at = excluded.restart_deadline_at,
                    failure_count = excluded.failure_count,
                    transition_count = excluded.transition_count
                """,
                (
                    projection.worker_id,
                    projection.restart_limit,
                    projection.restart_window_secs,
                    projection.healthy_reset_secs,
                    projection.is_open,
                    projection.revision,
                    projection.last_failure_at,
                    projection.restart_deadline_at,
                    projection.failure_count,
                    projection.transition_count,
                ),
            )
            connection.execute(
                'DELETE FROM worker_breaker_failures WHERE worker_id = ?',
                (projection.worker_id,),
            )
            connection.executemany(
                """
                INSERT INTO worker_breaker_failures (
                    worker_id, failure_index, failure_at
                ) VALUES (?, ?, ?)
                """,
                (
                    (projection.worker_id, index, failure_at)
                    for index, failure_at in enumerate(projection.failure_times)
                ),
            )
            connection.execute(
                'DELETE FROM worker_breaker_transitions WHERE worker_id = ?',
                (projection.worker_id,),
            )
            connection.executemany(
                """
                INSERT INTO worker_breaker_transitions (
                    worker_id, sequence, state, reason
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    (
                        projection.worker_id,
                        transition.sequence,
                        transition.state,
                        transition.reason,
                    )
                    for transition in projection.history
                ),
            )

        self._queue_write(('breaker', projection.worker_id), store)

    def promote_readded_group(
        self,
        *,
        site: str,
        group: str,
        source_state: SourceState,
        group_state: GroupState,
    ) -> bool:
        """Promote one re-add only after exact group and source terminal proof.

        Args:
            site: Site owning the pending group.
            group: Exact pending group identity.
            source_state: Open source authority providing terminal audit proof.
            group_state: Open group authority providing the matching
                disposition.

        Returns:
            Whether a pending re-add with matching terminal proofs was
            promoted.
            Missing pending inventory or either proof returns `False`.

        Raises:
            DatabaseError: A database operation fails or startup acquisition
                stops.
            RuntimeError: Inventory or source/group authority identities
                conflict.
            InvalidTransitionError: The group has conflicting terminal proofs.
            SourceIdentityError: Source terminal proof conflicts or is
                incomplete.
            ValueError: A retained group manifest or snapshot is invalid.
        """
        with self._startup_transaction(self._startup_stop or (lambda: False)) as connection:
            record = _inventory_records(connection).get((site, group))
            if record is None or record.lifecycle != 'readded-parked':
                return False
            orphan_generation = record.parked_orphan_generation()
            source = source_state.snapshot()
            group_snapshot = group_state.snapshot()
            if (
                source.site != site
                or source.source != record.source
                or group_snapshot.site != site
                or group_snapshot.source != record.source
                or group_snapshot.group != group
            ):
                raise RuntimeError('re-add promotion authority identity conflicts')
            identity = group_state.terminal_disposition_identity(record.generation)
            if identity is None:
                return False
            disposition_id, command_fingerprint, terminal_result = identity
            if not source_state.has_terminal_disposition(
                group,
                disposition_id,
                command_fingerprint,
                terminal_result,
            ):
                return False
            _promote_readded_inventory(
                connection,
                site=site,
                group=group,
                reserved_generation=record.generation,
                orphan_generation=orphan_generation,
            )
            return True

    def reconcile_groups(
        self,
        *,
        configured: Mapping[tuple[str, str], str],
        generations: Mapping[tuple[str, str], int],
    ) -> dict[tuple[str, str], SourceGroupAssignment]:
        """Reconcile configured groups before any worker spawn.

        Args:
            configured: Source names keyed by `(site, group)` for current
                groups.
            generations: Observed durable group generations under the same
                keys.

        Returns:
            Active or pending-disposition assignments keyed by `(site, group)`.
            Removed groups become orphaned; re-added groups remain parked until
            disposition supplies matching terminal proof.

        Raises:
            DatabaseError: SQLite fails or startup write acquisition is
                stopped.
            RuntimeError: Stored inventory is inconsistent or a configured
                group changes source without adoption.
            ValueError: A configured identity or observed generation is
                invalid.
        """
        assignments: dict[tuple[str, str], SourceGroupAssignment] = {}
        with self._startup_transaction(self._startup_stop or (lambda: False)) as connection:
            configured_keys = set(configured)
            stored = _inventory_records(connection)
            for (site, group), source in sorted(configured.items()):
                assignments[(site, group)] = _reconcile_inventory_group(
                    connection,
                    _InventoryReconciliation(
                        group=group,
                        group_generation=generations.get((site, group)),
                        previous=stored.get((site, group)),
                        site=site,
                        source=source,
                    ),
                )
            for site, group in set(stored).difference(configured_keys):
                previous = stored[(site, group)]
                if previous.orphan_generation is not None and (
                    not previous.configured
                    or generations.get((site, group)) == previous.generation
                ):
                    connection.execute(
                        """
                        UPDATE group_inventory
                        SET configured = 0, lifecycle = 'orphaned'
                        WHERE site = ? AND group_name = ?
                        """,
                        (site, group),
                    )
                    continue
                connection.execute(
                    """
                    UPDATE group_inventory
                    SET group_generation = COALESCE(
                            orphan_generation,
                            group_generation
                        ),
                        orphan_generation = NULL,
                        configured = 0,
                        lifecycle = 'orphaned'
                    WHERE site = ? AND group_name = ?
                    """,
                    (site, group),
                )
        return assignments

    def record_park_evidence(self, evidence: _ParkEvidence) -> bool:
        """Insert evidence only when its group has no unresolved row.

        Within `starting()`, commit before returning. Otherwise attempt the
        complete pending batch without waiting; contention leaves this change
        queued for `flush_pending()`.

        Args:
            evidence: Exact immutable pre-termination lane observation.

        Returns:
            `True` when accepted for immediate commit or queued persistence;
            `False` when unresolved evidence already exists for the group.

        Raises:
            DatabaseError: SQLite fails or startup write acquisition is
                stopped.
            sqlite3.Error: Reading existing durable evidence fails.
            Exception: Another queued transition fails while the batch is being
                applied.
        """
        if any(
            item.directive.site == evidence.directive.site
            and item.directive.group == evidence.directive.group
            for item in self.pending.parks.values()
        ):
            return False
        stored = self._database.connection.execute(
            'SELECT evidence_id FROM park_evidence WHERE site = ? AND group_name = ?',
            (evidence.directive.site, evidence.directive.group),
        ).fetchone()
        if stored is not None and ('retire', str(stored[0])) not in self.pending.writes:
            return False
        if self._startup_stop is None:
            self.pending.parks[evidence.directive.evidence_id] = evidence

        def store(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO park_evidence (
                    evidence_id, site, group_name, group_generation,
                    failed_worker_id, failed_spawn_instance, event_id,
                    group_revision, observed_at_wall_clock
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evidence.directive.evidence_id,
                    evidence.directive.site,
                    evidence.directive.group,
                    evidence.directive.group_generation,
                    evidence.failed_worker_id,
                    evidence.directive.failed_spawn_instance,
                    evidence.directive.event_id,
                    evidence.directive.group_revision,
                    evidence.directive.observed_at_wall_clock,
                ),
            )

        self._queue_write(('park', evidence.directive.evidence_id), store)
        return True

    def restore_worker_breaker(
        self,
        **options: Unpack[_WorkerBreakerRestoreOptions],
    ) -> _WorkerBreaker:
        """Restore one complete breaker or create a fresh configured one.

        A changed operating-system boot resets retained monotonic timing to the
        current clock. Current configured limits replace stored limits while
        retained failure and transition history is restored.

        Args:
            **options: Worker identity, restart limit, restart-window seconds,
                healthy-reset seconds, and optional monotonic clock and boot
                ID.

        Returns:
            The configured breaker with validated retained history, or a fresh
            breaker when no durable row exists.

        Raises:
            sqlite3.Error: Reading retained breaker or timing state fails.
            DatabaseError: Persisting timing or policy changes fails, or
                startup acquisition is stopped.
            RuntimeError: Boot identity cannot be obtained or retained breaker
                state, failure ordering, or transition history is inconsistent.
            ValueError: Configured breaker values, boot identity, or clock are
                invalid.
            Exception: Another queued transition fails during policy
                persistence.
        """
        worker_id = options['worker_id']
        restart_limit = options['restart_limit']
        restart_window_secs = options['restart_window_secs']
        healthy_reset_secs = options['healthy_reset_secs']
        clock = options.get('clock', time.monotonic)
        boot_id = options.get('boot_id')
        selected_boot_id = boot_identity() if boot_id is None else boot_id
        self._normalize_breaker_timing(selected_boot_id, clock())
        breaker = _WorkerBreaker(
            worker_id=worker_id,
            restart_limit=restart_limit,
            restart_window_secs=restart_window_secs,
            healthy_reset_secs=healthy_reset_secs,
            clock=clock,
        )
        row = self._database.connection.execute(
            """
            SELECT restart_limit, restart_window_secs, healthy_reset_secs,
                   is_open, revision, last_failure_at, restart_deadline_at,
                   failure_count, transition_count
            FROM worker_breaker
            WHERE worker_id = ?
            """,
            (worker_id,),
        ).fetchone()
        if row is None:
            return breaker
        failures = self._database.connection.execute(
            """
            SELECT failure_index, failure_at
            FROM worker_breaker_failures
            WHERE worker_id = ?
            ORDER BY failure_index
            """,
            (worker_id,),
        ).fetchall()
        if tuple(int(failure[0]) for failure in failures) != tuple(range(len(failures))):
            raise RuntimeError('durable worker breaker failure window is invalid')
        transitions: list[BreakerTransition] = []
        for transition in self._database.connection.execute(
            """
            SELECT sequence, state, reason
            FROM worker_breaker_transitions
            WHERE worker_id = ?
            ORDER BY sequence
            """,
            (worker_id,),
        ).fetchall():
            state = str(transition[1])
            transition_state: Literal['closed', 'open']
            if state == 'closed':
                transition_state = 'closed'
            elif state == 'open':
                transition_state = 'open'
            else:
                raise RuntimeError('durable worker breaker transition state is invalid')
            transitions.append(
                BreakerTransition(
                    sequence=int(transition[0]),
                    worker_id=worker_id,
                    state=transition_state,
                    reason=str(transition[2]),
                )
            )
        restored = _WorkerBreaker.from_projection(
            _WorkerBreakerProjection(
                worker_id=worker_id,
                restart_limit=restart_limit,
                restart_window_secs=restart_window_secs,
                healthy_reset_secs=healthy_reset_secs,
                is_open=bool(row[3]),
                revision=int(row[4]),
                last_failure_at=None if row[5] is None else float(row[5]),
                restart_deadline_at=None if row[6] is None else float(row[6]),
                failure_count=int(row[7]),
                failure_times=tuple(float(failure[1]) for failure in failures),
                transition_count=int(row[8]),
                history=tuple(transitions),
            ),
            clock=clock,
        )
        if (
            int(row[0]) != restart_limit
            or float(row[1]) != restart_window_secs
            or float(row[2]) != healthy_reset_secs
        ):
            self.persist_worker_breaker(restored)
        return restored

    def retire_park_evidence(self, evidence_id: str) -> None:
        """Delete one exact acknowledged evidence row.

        Within `starting()`, commit before returning. Otherwise attempt the
        complete pending batch without waiting; contention leaves this change
        queued for `flush_pending()`.

        Args:
            evidence_id: Exact acknowledged observation to retire; a
                still-pending insertion is cancelled instead of deleted from
                SQLite.

        Raises:
            DatabaseError: SQLite fails or startup write acquisition is
                stopped.
            RuntimeError: The durable observation changed before retirement.
            Exception: Another queued transition fails while the batch is being
                applied.
        """
        if self.pending.parks.pop(evidence_id, None) is not None:
            self.pending.writes.pop(('park', evidence_id))
            self.flush_pending()
            return

        def store(connection: sqlite3.Connection) -> None:
            deleted = connection.execute(
                'DELETE FROM park_evidence WHERE evidence_id = ?',
                (evidence_id,),
            )
            if deleted.rowcount != 1:
                raise RuntimeError('conditional park evidence changed before retirement')

        self._queue_write(('retire', evidence_id), store)

    @contextmanager
    def starting(self, should_stop: Callable[[], bool]) -> Generator[None]:
        """Use stop-aware durable writes before any worker can run."""
        previous = self._startup_stop
        self._startup_stop = should_stop
        try:
            yield
        finally:
            self._startup_stop = previous

    @contextmanager
    def _operator_transaction(self) -> Generator[sqlite3.Connection]:
        """Apply pending writes before validating one bounded operator transaction."""
        try:
            with self._write_transaction(2000) as connection:
                self.pending.apply(connection)
                yield connection
        except DatabaseError as error:
            if _supervisor_write_busy(error):
                raise _SupervisorDatabaseBusyError(
                    'supervisor-database-busy: supervisor.db is held by another connection'
                ) from error
            raise
        self.pending.clear()
        self._clear_write_stall()

    @classmethod
    def open(
        cls,
        state_dir: Path,
        *,
        synchronous: Literal['normal', 'full'] = 'normal',
        **options: Unpack[_SupervisorCheckpointOptions],
    ) -> Self:
        """Open or initialize durable supervisor state.

        Args:
            state_dir: Owner-only directory containing `supervisor.db`.
            synchronous: Durability mode for the supervisor connection.
            **options: Checkpoint interval in seconds, monotonic clock, and
                optional checkpointer factory.

        Returns:
            Supervisor state owning its database handle and checkpointer.

        Raises:
            SecurityError: The state directory or database path is unsafe.
            OSError: Resolving or preparing state paths fails.
            DatabaseError: Opening, initializing, or validating SQLite fails.
            ValueError: Checkpoint configuration is invalid.
            Exception: The supplied checkpointer factory fails.
        """
        directory = ensure_secure_state_dir(state_dir)
        database = open_database(
            directory / 'supervisor.db',
            'supervisor',
            _SUPERVISOR_SCHEMA,
            synchronous=synchronous.upper(),
        )
        try:
            return cls(database, **options)
        except BaseException:
            database.close()
            raise

    def reserve_group_disposition(
        self,
        *,
        site: str,
        group: str,
        observed_generation: int,
    ) -> PendingDispositionSourceGroupAssignment:
        """Reserve or replay one durable old-to-new operator generation fence.

        Args:
            site: Exact target site.
            group: Exact target group.
            observed_generation: Generation observed from the group authority;
                a new reservation exceeds this and every retained generation.

        Returns:
            The existing pending reservation or a newly committed parked
            reservation carrying both old and replacement generations.

        Raises:
            ValueError: The group is unknown or an identity or generation is
                invalid.
            RuntimeError: Durable inventory or its orphan-generation fence
                conflicts.
            DatabaseError: Applying pending writes or committing the
                reservation fails.
            ControlCommandError: The bounded supervisor-database acquisition
                expires; the retryable result carries code
                `supervisor-database-busy`.
            Exception: A pending transition fails before reservation
                validation.
        """
        with self._operator_transaction() as connection:
            record = _inventory_records(connection).get((site, group))
            if record is None:
                raise ValueError('group disposition target is unknown')
            if not record.configured:
                if record.lifecycle != 'orphaned':
                    raise RuntimeError('absent group disposition inventory is invalid')
                orphan_generation = record.orphan_generation
                if orphan_generation is not None:
                    return PendingDispositionSourceGroupAssignment(
                        source=record.source,
                        group=group,
                        orphan_generation=orphan_generation,
                        reserved_generation=record.generation,
                    )
                reserved_generation = _allocate_generation(
                    connection,
                    site=site,
                    group=group,
                    observed_generations=_observed_generations(
                        record.generation, observed_generation
                    ),
                )
                connection.execute(
                    """
                    UPDATE group_inventory
                    SET group_generation = ?,
                        orphan_generation = ?,
                        start_mode = 'resume'
                    WHERE site = ? AND group_name = ?
                      AND group_generation = ?
                      AND configured = 0
                      AND lifecycle = 'orphaned'
                    """,
                    (
                        reserved_generation,
                        record.generation,
                        site,
                        group,
                        record.generation,
                    ),
                )
                return PendingDispositionSourceGroupAssignment(
                    source=record.source,
                    group=group,
                    orphan_generation=record.generation,
                    reserved_generation=reserved_generation,
                )
            if record.lifecycle == 'readded-parked':
                orphan_generation = record.orphan_generation
                if orphan_generation is None:
                    raise RuntimeError('pending disposition lacks an orphan generation')
                return PendingDispositionSourceGroupAssignment(
                    source=record.source,
                    group=group,
                    orphan_generation=orphan_generation,
                    reserved_generation=record.generation,
                )
            if record.lifecycle != 'active' or record.orphan_generation is not None:
                raise RuntimeError('group disposition inventory is not active')
            reserved_generation = _allocate_generation(
                connection,
                site=site,
                group=group,
                observed_generations=_observed_generations(record.generation, observed_generation),
            )
            connection.execute(
                """
                UPDATE group_inventory
                SET group_generation = ?,
                    orphan_generation = ?,
                    start_mode = 'initialize',
                    lifecycle = 'readded-parked'
                WHERE site = ? AND group_name = ?
                  AND group_generation = ?
                  AND configured = 1
                  AND lifecycle = 'active'
                """,
                (
                    reserved_generation,
                    record.generation,
                    site,
                    group,
                    record.generation,
                ),
            )
            return PendingDispositionSourceGroupAssignment(
                source=record.source,
                group=group,
                orphan_generation=record.generation,
                reserved_generation=reserved_generation,
            )

    def reset_worker_breaker(
        self,
        breaker: _WorkerBreaker,
        *,
        expected_revision: int,
        reason: str,
        identity: AuditIdentity,
        wall_clock_time: float,
    ) -> BreakerTransition:
        """Durably audit and apply one exact worker-breaker reset.

        Args:
            breaker: Current in-memory projection of the open breaker.
            expected_revision: Required in-memory and durable breaker revision.
            reason: Nonempty operator reason bounded to 4096 UTF-8 bytes.
            identity: Process identity recorded in the audit row.
            wall_clock_time: Audit observation in seconds since the Unix epoch.

        Returns:
            The committed closed transition, also applied to the supplied
            breaker.

        Raises:
            ValueError: Audit time, reason, revision, or open-breaker state is
                invalid.
            DatabaseError: Applying pending writes or committing the reset
                fails.
            ControlCommandError: The bounded supervisor-database acquisition
                expires; the retryable result carries code
                `supervisor-database-busy`.
            Exception: A pending transition fails while applying the reset
                transaction.
        """
        if not isfinite(wall_clock_time) or wall_clock_time < 0:
            raise ValueError('worker breaker audit time must be nonnegative')
        breaker.validate_reset(
            expected_revision=expected_revision,
            reason=reason,
        )
        transition_sequence = breaker.transition_count + 1
        with self._operator_transaction() as connection:
            updated = connection.execute(
                """
                UPDATE worker_breaker
                SET is_open = 0,
                    revision = revision + 1,
                    last_failure_at = NULL,
                    restart_deadline_at = NULL,
                    transition_count = transition_count + 1
                WHERE worker_id = ?
                  AND revision = ?
                  AND is_open = 1
                  AND transition_count = ?
                """,
                (
                    breaker.worker_id,
                    expected_revision,
                    breaker.transition_count,
                ),
            )
            if updated.rowcount != 1:
                raise ValueError('worker breaker reset revision conflicts')
            connection.execute(
                'DELETE FROM worker_breaker_failures WHERE worker_id = ?',
                (breaker.worker_id,),
            )
            connection.execute(
                """
                INSERT INTO worker_breaker_transitions (
                    worker_id, sequence, state, reason
                ) VALUES (?, ?, 'closed', ?)
                """,
                (breaker.worker_id, transition_sequence, reason),
            )
            connection.execute(
                """
                DELETE FROM worker_breaker_transitions
                WHERE worker_id = ? AND sequence <= ?
                """,
                (breaker.worker_id, transition_sequence - _WORKER_BREAKER_HISTORY_LIMIT),
            )
            connection.execute(
                """
                INSERT INTO breaker_audit (
                    worker_id, prior_revision, new_revision, command, reason,
                    wall_clock_time, local_uid, username, pid
                ) VALUES (?, ?, ?, 'reset-worker-breaker', ?, ?, ?, ?, ?)
                """,
                (
                    breaker.worker_id,
                    expected_revision,
                    expected_revision + 1,
                    reason,
                    wall_clock_time,
                    identity.local_uid,
                    identity.username,
                    identity.pid,
                ),
            )
        return breaker.apply_audited_reset(
            expected_revision=expected_revision,
            reason=reason,
        )


class _WorkerHandle(Protocol):
    """Define the spawned worker surface advanced through staged startup."""

    spawn_instance: str
    spec: WorkerSpec

    def assign(self, assignment: WorkerAssignment) -> None:
        """Send the worker's single immutable assignment.

        A send timeout or transport failure poisons the channel; assignment is
        recorded only after a complete successful send.

        Args:
            assignment: Exact immutable assignment matching this worker and
                spawn instance.

        Raises:
            ValueError: An assignment was already delivered, its spawn identity
                conflicts, or its control frame cannot be encoded within the
                configured limit.
            RuntimeError: The child has not authenticated.
            TimeoutError: The complete send deadline expires.
            EOFError: The control channel or peer is closed.
            OSError: The control transport fails.
        """
        ...

    def authenticate(self, *, timeout: float = 0.0) -> bool:
        """Advance exact child authentication without exceeding `timeout`.

        Args:
            timeout: Maximum wait in seconds for the first complete message;
                zero only checks ready input.

        Returns:
            `True` after exact child authentication, including repeated calls;
            `False` if the complete-message wait expires.

        Raises:
            EOFError: The channel closes or its receive transport fails.
            TypeError: The first typed message is not `ControlHello`.
            ValueError: The timeout or received frame is invalid, or the
                hello's worker, spawn, or process identity conflicts.
        """
        ...

    def close(self) -> None:
        """Close the worker control channel."""
        ...

    def has_pending_input(self) -> bool:
        """Check waiting control input without consuming or refreshing arrival.

        Raises:
            EOFError: The channel is closed or its receive transport fails.
        """
        ...

    def kill(self) -> None:
        """Force termination without waiting for worker cleanup."""
        ...

    @property
    def last_arrival_at(self) -> float | None:
        """Return the latest control byte arrival, if any."""
        ...

    def receive(self, *, timeout: float) -> ControlMessage:
        """Receive one typed worker message.

        Args:
            timeout: Finite nonnegative timeout in seconds for the complete
                receive operation.

        Returns:
            The next typed child control message.

        Raises:
            EOFError: The channel closes or its receive transport fails.
            TimeoutError: No complete message arrives within the timeout.
            ValueError: The peer sends an invalid frame.
        """
        ...

    def receive_available(self) -> Iterator[ControlMessage]:
        """Freeze one finite ordered batch for the sole channel reader.

        Acquisition occurs before iteration. The sole reader must exhaust this
        iterator before another receive operation; later-arriving bytes belong
        to a later batch.

        Returns:
            An ordered iterator over the entry message and byte snapshot.
            Earlier messages precede a later frame error or transport EOF
            raised during iteration.

        Raises:
            EOFError: The channel is closed or transport inspection fails.
            ValueError: A received frame is invalid.
        """
        ...

    def send(self, message: ControlMessage) -> None:
        """Send one typed post-assignment control message.

        A send timeout or transport failure permanently poisons the channel.

        Args:
            message: Typed control value sent after immutable assignment.

        Raises:
            ValueError: The frame cannot be encoded within the configured
                limit.
            RuntimeError: The child has not received its assignment.
            TimeoutError: The complete send deadline expires.
            EOFError: The control channel or peer is closed.
            OSError: The control transport fails.
        """
        ...

    def shutdown(self, reason: str) -> int | None:
        """Stop and reap the worker within its deadline.

        Args:
            reason: Nonempty body-free reason included in the stop command.

        Returns:
            Exit status, or `None` if the child is still unreaped after
            graceful and forced-stop waits. Each wait has its own configured
            shutdown deadline; the control channel is closed before normal
            return.

        Raises:
            OSError: Force-signaling the child fails.
            Exception: Configured foreground progress reporting fails.
        """
        ...

    def wait(self, *, timeout: float = 0.0) -> int | None:
        """Return the child status within one bounded wait.

        Args:
            timeout: Nonnegative maximum wait in seconds; zero polls without
                waiting.

        Returns:
            The child exit status, or `None` when it has not exited within the
            timeout.
        """
        ...

    @property
    def worker_id(self) -> str:
        """Return the packed worker identity."""
        ...


class _WorkerSpawnOptions(TypedDict):
    """Type one exact exec-worker launch request."""

    config_path: Path
    spec: WorkerSpec
    spawn_instance: str
    max_frame_bytes: int
    send_deadline_secs: float
    startup_deadline_secs: float
    shutdown_deadline_secs: float
    foreground_progress: NotRequired[Callable[[], None] | None]
    wait_slice_secs: NotRequired[float]
    env: NotRequired[Mapping[str, str] | None]


class _WorkerFactory(Protocol):
    """Spawn a worker from the complete supported launch request."""

    def __call__(self, **options: Unpack[_WorkerSpawnOptions]) -> _WorkerHandle:
        """Return one unauthenticated spawned worker."""
        ...


class _SupervisorOptions(TypedDict):
    """Type the complete supervisor construction request."""

    config: Config
    config_path: Path
    clock: NotRequired[Callable[[], float]]
    wall_clock: NotRequired[Callable[[], float]]
    checkpointer_factory: NotRequired[Callable[..., Checkpointer]]
    worker_factory: NotRequired[_WorkerFactory | None]
    token_factory: NotRequired[Callable[[], str] | None]


class _WorkerControlError(Exception):
    """Carry one worker control-channel failure."""

    def __init__(
        self,
        worker: _WorkerHandle,
        error: OSError | EOFError | ValueError,
        *,
        sending: bool,
    ) -> None:
        """Retain the affected worker, failure, and send-phase flag."""
        self.error = error
        self.sending = sending
        self.worker = worker


@dataclass
class _WorkerLifecycle:
    """Track one worker's own startup steps and endpoint readiness."""

    assignment: WorkerAssignment
    deadline_at: float
    worker: _WorkerHandle
    step_started_at: float
    assigned: bool = False
    authenticated: bool = False
    endpoint_messages: dict[str, SourceEndpointReady] = field(
        default_factory=dict[str, SourceEndpointReady]
    )
    grant_index: int = 0
    grant_sent: bool = False
    ready: bool = False

    def active_grant(self) -> EventPeerGrant | None:
        """Return the sent grant still awaiting the source owner's reply."""
        grants = self.assignment.event_grants
        if not self.grant_sent or self.grant_index >= len(grants):
            return None
        return grants[self.grant_index]


@dataclass
class _WorkerSpawn:
    """Own all ephemeral state for one exact worker process."""

    worker: _WorkerHandle
    health_deadline: float
    pending_groups: frozenset[str]
    lifecycle: _WorkerLifecycle | None
    pending_recovery: dict[str, ConditionalParkDirective] = field(
        default_factory=dict[str, ConditionalParkDirective]
    )
    attributed_park_failure: bool = False
    health: HealthUpdate | None = None
    health_failure: str | None = None
    health_inventory_complete: bool = False
    termination_reason: str | None = None


class Supervisor:
    """Own the unopened supervisor lifecycle and configured worker children.

    Context entry acquires the state-dir authority. Call
    `reconcile_durable_state()` before `start()`; startup is asynchronous, and
    `run()` drives supervision. Context exit stops owned children and releases
    authority.
    """

    def __init__(
        self,
        **options: Unpack[_SupervisorOptions],
    ) -> None:
        """Configure an unopened supervisor.

        Args:
            **options: Complete configuration and injectable runtime seams.
                `config` is the resolved parent configuration, while spawned
                children reload `config_path`.
                `clock` supplies monotonic seconds for supervision, and
                `wall_clock` supplies epoch seconds for durable observations.
                `worker_factory` returns a spawned, unauthenticated and
                unassigned handle for staged startup; `token_factory` supplies
                opaque lifecycle identities. `checkpointer_factory` builds the
                periodic WAL checkpointer.
        """
        config = options['config']
        config_path = options['config_path']
        clock = options.get('clock', time.monotonic)
        wall_clock = options.get('wall_clock', time.time)
        worker_factory = options.get('worker_factory')
        token_factory = options.get('token_factory')
        self._adopting_sources: set[tuple[str, str]] = set()
        self._clock = clock
        self._breakers: dict[str, _WorkerBreaker] = {}
        self._config = config
        self._config_path = config_path
        self._dispatch_reserved: _WorkerHandle | None = None
        self._maintenance_failure: BaseException | None = None
        self._should_stop: Callable[[], bool] | None = None
        self._halt_reason_value: str | None = None
        self._message_batches: dict[str, Iterator[ControlMessage]] | None = None
        self._batch_message_count = 0
        self._lock: FileLock | None = None
        self._next_checkpoint_at: float | None = None
        self._checkpointer_factory = options.get('checkpointer_factory', Checkpointer)
        self._operator_request_sequence = 0
        self._park_evidence_by_group: dict[tuple[str, str], _ParkEvidence] = {}
        self._pending_dependents: dict[str, WorkerSpec] = {}
        self._pending_disposition_resumes: dict[
            tuple[str, str, int], tuple[ActiveSourceGroupAssignment, CommandResult]
        ] = {}
        control_progress_budget = max(
            config.daemon.supervisor_poll_interval_secs,
            2 * config.daemon.health_interval_secs,
            _control.REQUEST_TIMEOUT_SECS,
            BUSY_TIMEOUT_SECS,
        )
        self._progress = SupervisorProgress(
            supervisor_stale_after=2 * control_progress_budget,
            readiness_snapshot_stale_after=(
                2
                * max(
                    config.daemon.health_interval_secs,
                    config.daemon.supervisor_poll_interval_secs,
                )
            ),
        )
        self._reconciled: dict[tuple[str, str], SourceGroupAssignment] | None = None
        self._restart_deadlines: dict[str, float] = {}
        self._source_endpoints: dict[tuple[str, str], SourceEndpointReady] = {}
        self._source_workers: dict[tuple[str, str], _WorkerHandle] = {}
        self._specs: dict[str, WorkerSpec] = {}
        self._stale_grant_readies: set[EventPeerReady] = set()
        self._state: _SupervisorState | None = None
        self._shutting_down = False
        self._token_factory = (
            (lambda: uuid.uuid4().hex) if token_factory is None else token_factory
        )
        self._worker_factory = _WorkerInstance.spawn if worker_factory is None else worker_factory
        self._spawns: dict[str, _WorkerSpawn] = {}
        self._wall_clock = wall_clock

    def __enter__(self) -> Self:
        """Open and return this supervisor.

        Returns:
            This supervisor while it owns the state-dir lock.

        Raises:
            AlreadyLockedError: Another supervisor holds the state-dir lock.
            TimeoutError: Old worker locks remain held beyond the takeover
                deadline.
            SecurityError: A state or lock path cannot be securely opened or
                fails filesystem safety checks.
            DatabaseError: Supervisor state cannot be opened or validated.
            ValueError: The supervisor clock, checkpoint configuration, or
                retained park evidence is invalid.
            sqlite3.Error: Reading retained park evidence fails.
            OSError: Preparing state paths or acquiring the supervisor lock
                fails.
            Exception: The supplied checkpointer factory fails.
        """
        self._open()
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Close workers and release the state-dir lock.

        Raises:
            RuntimeError: An owned child remains unreaped after forced
                termination.
            OSError: Signaling a child or closing owned descriptors fails.
            Exception: An injected worker shutdown or foreground-progress
                operation fails.
        """
        self._close()

    def _accept_group_recovery_ready(
        self,
        worker: _WorkerHandle,
        message: GroupRecoveryReady,
    ) -> None:
        """Retire exact durable evidence from its authenticated current spawn."""
        spawn = self._spawns.get(worker.worker_id)
        lifecycle = None if spawn is None else spawn.lifecycle
        if (
            spawn is None
            or spawn.worker is not worker
            or spawn.termination_reason is not None
            or (
                lifecycle is not None
                and (
                    not lifecycle.authenticated
                    or not lifecycle.assigned
                    or lifecycle.assignment.spawn_instance != worker.spawn_instance
                )
            )
            or message.spawn_instance != worker.spawn_instance
        ):
            raise ValueError('group recovery ready spawn is not authenticated current authority')
        directive = spawn.pending_recovery.get(message.evidence_id)
        if directive is None:
            raise ValueError('group recovery ready is duplicate or conflicts')
        if (
            message.site != directive.site
            or message.site != worker.spec.site
            or message.group != directive.group
            or message.group_generation != directive.group_generation
        ):
            raise ValueError('group recovery ready identity conflicts')
        if (
            message.outcome == 'parked'
            and message.resulting_revision != directive.group_revision + 1
        ) or (
            message.outcome == 'stale' and message.resulting_revision <= directive.group_revision
        ):
            raise ValueError('group recovery ready revision conflicts')
        evidence = self._park_evidence_by_group.get((directive.site, directive.group))
        if evidence is None or evidence.directive != directive:
            raise ValueError('group recovery ready evidence conflicts with current authority')
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        durable = next(
            (
                item
                for item in (*state.pending_park_evidence(), *state.pending.parks.values())
                if item.directive.evidence_id == directive.evidence_id
            ),
            None,
        )
        if durable is None or durable.directive != directive:
            raise ValueError('group recovery ready durable evidence is unavailable')
        state.retire_park_evidence(directive.evidence_id)
        self._park_evidence_by_group.pop((directive.site, directive.group))
        spawn.pending_recovery.pop(directive.evidence_id)

    def _active_dependent_grant_owner(
        self,
        source_worker: _WorkerHandle,
    ) -> _WorkerLifecycle | None:
        """Return the lifecycle currently consuming one source control channel."""
        for spawn in self._spawns.values():
            lifecycle = spawn.lifecycle
            if lifecycle is None:
                continue
            grant = lifecycle.active_grant()
            if grant is None:
                continue
            owner_worker = self._source_workers.get((lifecycle.worker.spec.site, grant.source))
            if owner_worker is source_worker:
                return lifecycle
        return None

    def _advance_progress(self) -> None:
        """Record one completed bounded foreground step."""
        self._progress.advance(now=self._clock())

    def _complete_worker_lifecycle(self, lifecycle: _WorkerLifecycle) -> None:
        """Release readiness independently of lane-owned recovery work."""
        if lifecycle.ready:
            self._spawns[lifecycle.worker.worker_id].lifecycle = None

    @staticmethod
    def _current_disposition_proof(
        source_state: SourceState, group_state: GroupState, generation: int
    ) -> bool:
        """Require exact terminal audit proof at the current stored generation."""
        if group_state.snapshot().group_generation != generation:
            return False
        identity = group_state.terminal_disposition_identity(generation)
        return identity is not None and source_state.has_terminal_disposition(
            group_state.snapshot().group, *identity
        )

    def _completed_absent_disposition(
        self, record: _InventoryRecord, source_state: SourceState, group_state: GroupState
    ) -> bool:
        """Distinguish a completed absent disposition from a new configuration removal."""
        return (
            record.orphan_generation is None
            and group_state.lifecycle() == 'active'
            and self._current_disposition_proof(source_state, group_state, record.generation)
        )

    def _dependent_sources_ready(self, spec: WorkerSpec) -> bool:
        """Check that every dependent group has a registered source owner."""
        return all(
            (spec.site, self._config.sites[spec.site].groups[group].source) in self._source_workers
            for group in spec.groups
        )

    def _ensure_worker_breaker(self, spec: WorkerSpec) -> _WorkerBreaker:
        """Return one restored breaker before a child can fail."""
        if spec.worker_id not in self._breakers:
            configured = (
                self._config.sites[spec.site].workers.get(spec.name)
                if spec.kind.value == 'named'
                else None
            )
            policy = WorkerConfig(name=spec.name) if configured is None else configured
            state = self._state
            if state is None:
                raise RuntimeError('supervisor state is not open')
            self._breakers[spec.worker_id] = state.restore_worker_breaker(
                worker_id=spec.worker_id,
                restart_limit=policy.restart_limit,
                restart_window_secs=policy.restart_window_secs,
                healthy_reset_secs=policy.healthy_reset_secs,
                clock=self._clock,
            )
        return self._breakers[spec.worker_id]

    def _existing_worker_lock_paths(self) -> list[tuple[Path, bool]]:
        """List existing worker lock paths and identify write-lock files."""
        paths: list[tuple[Path, bool]] = []
        for site in self._config.sites.values():
            for source in site.sources.values():
                source_lock = source_lock_path(
                    source_database_path(self._config, site.name, source.name)
                )
                write_lock = source_write_lock_path(self._config, site.name, source.name)
                if source_lock.exists():
                    paths.append((source_lock, False))
                if write_lock.exists():
                    paths.append((write_lock, True))
            group_dir = site_groups_directory(self._config, site.name)
            paths.extend((path, False) for path in group_dir.glob('*.lock'))
        return paths

    def _grant_worker_ids(self) -> set[str]:
        """Return source workers currently serving dependent grants."""
        worker_ids: set[str] = set()
        for worker_id, spawn in self._spawns.items():
            lifecycle = spawn.lifecycle
            if lifecycle is None:
                continue
            grant = lifecycle.active_grant()
            if grant is None:
                continue
            source_worker = self._source_workers.get((lifecycle.worker.spec.site, grant.source))
            if source_worker is None:
                if self._spawns[worker_id].termination_reason is not None:
                    continue
                raise RuntimeError('active dependent grant source owner is unavailable')
            worker_ids.add(source_worker.worker_id)
        return worker_ids

    @staticmethod
    def _group_disposition_target(command: OperatorCommand) -> tuple[str, str]:
        """Validate and return one exact group disposition target."""
        expected = GROUP_DISPOSITION_ARGUMENTS.get(command.command)
        if expected is None:
            raise ValueError('group disposition command is invalid')
        if frozenset(command.arguments) != expected:
            raise ValueError('group disposition arguments are invalid')
        site = command.arguments['site']
        group = command.arguments['group']
        if not isinstance(site, str) or not isinstance(group, str):
            raise TypeError('group disposition target is invalid')
        return site, group

    def _group_source_path(self, site: str, group: str) -> Path:
        """Resolve configured groups or orphan inventory to their source file."""
        configured_site = self._config.sites.get(site)
        if configured_site is not None and group in configured_site.groups:
            source = configured_site.groups[group].source
        else:
            state = self._state
            record = (
                None if state is None else state.group_inventory_record(site=site, group=group)
            )
            if record is None:
                raise RuntimeError('absent orphan group inventory is missing')
            source = record.source
        return source_database_path(self._config, site, source)

    def _disposition_observation(
        self, site: str, group: str, disposition_id: str
    ) -> tuple[tuple[int, int] | None, int, bool]:
        """Read receipt, revision, and command admission without mutating source state."""
        path = self._group_source_path(site, group)
        with closing(_open_readonly_source(path)) as connection:
            observation = _source_store.disposition_observation(connection, group, disposition_id)
            if observation is None:
                raise RuntimeError('group disposition state is unavailable')
            receipt, revision, parked, lifecycle = observation
            return (
                receipt,
                revision,
                disposition_admitted(parked=parked, lifecycle=lifecycle),
            )

    def _durable_restart_start_mode(
        self,
        site: str,
        assignment: ActiveSourceGroupAssignment,
    ) -> StartMode:
        """Classify restart from the durable group FULL bootstrap."""
        path = self._group_source_path(site, assignment.group)
        if not path.is_file():
            if assignment.start_mode == 'initialize':
                return 'initialize'
            raise RuntimeError('resume group state is missing')
        try:
            connection = _open_readonly_source(path)
            try:
                row = _source_store.restart_group_identity(connection, assignment.group)
            finally:
                connection.close()
        except sqlite3.Error as error:
            raise RuntimeError(f'cannot classify durable group restart: {path}') from error
        if row is None:
            if assignment.start_mode == 'initialize':
                return 'initialize'
            raise RuntimeError('resume group state is missing')
        if (
            str(row[0]) != site
            or str(row[1]) != assignment.group
            or int(row[2]) != assignment.group_generation
        ):
            raise RuntimeError('durable group restart identity conflicts')
        return 'resume'

    def _build_worker_assignment(
        self,
        spec: WorkerSpec,
        spawn_instance: str,
        source_generations: tuple[tuple[str, str], ...],
        source_endpoints: tuple[SourceEndpointReady, ...],
        event_grants: tuple[EventPeerGrant, ...],
    ) -> WorkerAssignment:
        """Build one cold-or-replacement assignment from current authority."""
        if self._reconciled is None:
            raise RuntimeError('supervisor state is not reconciled')
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        reconciled = state.configured_group_assignments()
        expected_groups = {
            (site.name, group.name)
            for site in self._config.sites.values()
            for group in site.groups.values()
        }
        if set(reconciled) != expected_groups:
            raise RuntimeError('durable supervisor inventory conflicts with configuration')
        self._reconciled = reconciled
        site = self._config.sites[spec.site]
        group_assignments: list[DeliveryGroupAssignment] = []
        for group_name in spec.groups:
            inventory = reconciled[(site.name, group_name)]
            if isinstance(inventory, PendingDispositionSourceGroupAssignment):
                continue
            evidence = self._park_evidence_by_group.get((site.name, group_name))
            directive = None
            if evidence is not None:
                if evidence.directive.group_generation != inventory.group_generation:
                    raise RuntimeError(
                        f'conditional park evidence generation conflicts: {site.name}/{group_name}'
                    )
                directive = evidence.directive
            group_assignments.append(
                DeliveryGroupAssignment(
                    source=inventory.source,
                    group=inventory.group,
                    group_generation=inventory.group_generation,
                    start_mode=self._durable_restart_start_mode(site.name, inventory),
                    conditional_park=directive,
                )
            )
        source_inventory = tuple(
            (
                replace(
                    inventory,
                    start_mode=self._durable_restart_start_mode(
                        site.name,
                        inventory,
                    ),
                )
                if isinstance(inventory, ActiveSourceGroupAssignment)
                else inventory
            )
            for group in sorted(site.groups.values(), key=lambda item: item.name)
            if group.source in spec.sources
            for inventory in (reconciled[(site.name, group.name)],)
        )
        return WorkerAssignment(
            worker_id=spec.worker_id,
            spawn_instance=spawn_instance,
            source_generations=source_generations,
            group_assignments=tuple(group_assignments),
            source_group_assignments=source_inventory,
            source_endpoints=source_endpoints,
            event_grants=event_grants,
        )

    @staticmethod
    def _group_state_exists(path: Path, group: str) -> bool:
        """Check the group row in an existing source without creating a file."""
        if not path.is_file():
            return False
        connection = _open_readonly_source(path)
        try:
            return _source_store.group_exists(connection, group)
        finally:
            connection.close()

    def _internal_watchdog_budget(self, site_name: str, source_name: str) -> float:
        """Return the shared internal budget for one configured source."""
        site = self._config.sites[site_name]
        source = site.sources[source_name]
        credential = site.credentials[source.credentials]
        return internal_stall_deadline_secs(
            self._config.daemon.health_interval_secs,
            credential,
        )

    def _inventory_failure_reason(
        self,
        worker: _WorkerHandle,
        inventory: _HealthInventory,
    ) -> str | None:
        """Return the reason for one incomplete configured liveness inventory."""
        expected_sources = set(worker.spec.sources)
        expected_groups = set(worker.spec.groups)
        if (
            set(inventory.source_records) != expected_sources
            or set(inventory.source_watchdogs) != expected_sources
        ):
            return 'source-phase-overdue'
        if set(inventory.lane_watchdogs) != expected_groups:
            return 'lane-phase-overdue'
        if inventory.operator_count != 1:
            return 'operator-phase-overdue'
        return None

    def _lifecycle_unclocked(self, lifecycle: _WorkerLifecycle) -> bool:
        """Exclude supervisor and source work between hello and assignment."""
        return lifecycle.authenticated and not lifecycle.assigned

    def _next_operator_transport_command(
        self,
        command: OperatorCommand,
    ) -> OperatorCommand:
        """Copy an operator command with a fresh transport request ID."""
        self._operator_request_sequence += 1
        return replace(
            command,
            request_id=f'operator-{self._operator_request_sequence}',
        )

    def _next_token(self) -> str:
        """Return one validated non-reusable supervisor identity."""
        token = self._token_factory()
        if type(token) is not str or not token:
            raise ValueError('supervisor token factory returned an invalid token')
        return token

    def _omitted_park_evidence(
        self, site: str, group: str, generations: tuple[int, int | None]
    ) -> _ParkEvidence | None:
        """Validate retained park evidence before retiring an omitted group's intent."""
        evidence = self._park_evidence_by_group.get((site, group))
        if evidence is not None and evidence.directive.group_generation not in generations:
            raise RuntimeError(f'omitted group evidence generation conflicts: {site}/{group}')
        return evidence

    def _operator_command_owner(self, command: OperatorCommand) -> _WorkerHandle:
        """Return the single running owner for one operator command."""
        site = command.arguments.get('site')
        source = command.arguments.get('source')
        group = command.arguments.get('group')
        if not isinstance(site, str):
            raise TypeError('operator command site is invalid')
        if command.command == 'unpark' and isinstance(group, str):
            configured_site = self._config.sites.get(site)
            configured_group = (
                None if configured_site is None else configured_site.groups.get(group)
            )
            if (
                configured_group is not None
                and (site, configured_group.source) in self._adopting_sources
            ):
                raise ValueError('group cannot unpark during source adoption')
            state = self._state
            assignment = (
                None if state is None else state.configured_group_assignments().get((site, group))
            )
            if isinstance(
                assignment,
                PendingDispositionSourceGroupAssignment,
            ):
                raise ValueError('group cannot unpark before its source-adoption disposition')
        owners = tuple(
            worker
            for spawn in self._spawns.values()
            for worker in (spawn.worker,)
            if worker.spec.site == site
            and self._spawns[worker.worker_id].lifecycle is None
            and self._spawns[worker.worker_id].termination_reason is None
            and worker.worker_id not in self._grant_worker_ids()
            and (
                (
                    isinstance(source, str)
                    and (group is None or command.command == 'group-disposition')
                    and source in worker.spec.sources
                )
                or (isinstance(group, str) and source is None and group in worker.spec.groups)
            )
        )
        if len(owners) != 1:
            configured_site = self._config.sites.get(site)
            if configured_site is None:
                raise ValueError(f'operator target site {site!r} is not configured; check --site')
            if isinstance(group, str) and group not in configured_site.groups:
                raise ValueError(
                    f'operator target group {site}/{group} is not configured; check --group'
                )
            if isinstance(source, str) and source not in configured_site.sources:
                raise ValueError(
                    f'operator target source {site}/{source} is not configured; check --source'
                )
            target = f'{site}/{group or source}'
            raise ValueError(
                f'operator target {target} has no running worker '
                '(restarting or breaker open); check status'
            )
        return owners[0]

    def _orphan_group_metadata(
        self,
        *,
        site: str,
        group: str,
    ) -> _OrphanGroupMetadata:
        """Read one absent orphan's durable open settings."""
        path = self._group_source_path(site, group)
        connection = _open_readonly_source(path)
        try:
            row = _source_store.orphan_group_metadata(connection, group)
        finally:
            connection.close()
        if row is None:
            raise RuntimeError('absent orphan group metadata is missing')
        ordering = str(row[1])
        if ordering not in {'eager', 'strict'}:
            raise RuntimeError('absent orphan ordering is invalid')
        if row[5] is None:
            raise RuntimeError(f'durable group source incarnation is unavailable: {site}/{group}')
        return _OrphanGroupMetadata(
            group_generation=int(row[0]),
            ordering=cast("Literal['eager', 'strict']", ordering),
            site=str(row[2]),
            source=str(row[3]),
            source_generation=str(row[4]),
            verified_incarnation=row[5],
        )

    def _pending_worker_groups(self, worker: _WorkerHandle) -> set[str]:
        """Return source-owned status records absent from this worker's lane inventory."""
        spawn = self._spawns.get(worker.worker_id)
        if spawn is None or spawn.worker is not worker:
            return set()
        return set(spawn.pending_groups)

    @staticmethod
    def _phase_confirmed_overdue(heartbeat_at: float, deadline_at: float | None) -> bool:
        """Confirm phase expiry only with an observation at or beyond its deadline."""
        return deadline_at is not None and heartbeat_at >= deadline_at

    @staticmethod
    @contextmanager
    def _normalize_worker_control_errors(
        worker: _WorkerHandle,
        *,
        sending: bool = False,
    ) -> Generator[None]:
        """Wrap control-channel failures from one exact worker operation."""
        try:
            yield
        except TimeoutError as error:
            if not sending:
                raise
            raise _WorkerControlError(worker, error, sending=True) from error
        except EOFError as error:
            raise _WorkerControlError(worker, error, sending=sending) from error
        except OSError as error:
            raise _WorkerControlError(worker, error, sending=sending) from error
        except ValueError as error:
            raise _WorkerControlError(worker, error, sending=sending) from error

    @staticmethod
    def _orphan_group(
        source_state: SourceState,
        group_state: GroupState,
        *,
        transition_generation: int | None = None,
    ) -> ReplayRequiredNotice:
        """Retire one generation to matching source and group orphan state."""
        source = source_state.snapshot()
        group = group_state.snapshot()
        if (
            source.site != group.site
            or source.source != group.source
            or source.source_generation != group.source_generation
        ):
            raise ValueError('source and group orphan identities conflict')
        lifecycle = group_state.lifecycle()
        if lifecycle != 'orphaned':
            group_state.change_lifecycle(
                LifecycleRequest(
                    command='orphan',
                    expected_lifecycle=lifecycle,
                    expected_generation=group.group_generation,
                    transition_generation=(
                        group.group_generation
                        if transition_generation is None
                        else transition_generation
                    ),
                    identity=AuditIdentity.current(),
                    wall_clock_time=time.time(),
                )
            )
        return next(
            marker
            for marker in source_state.snapshot().replay_markers
            if marker.group == group.group and marker.group_generation == group.group_generation
        )

    @staticmethod
    def _check_source_incarnation(source_state: SourceState, group_state: GroupState) -> None:
        """Preserve the startup stream fence without any disposition repair phase."""
        source = source_state.snapshot()
        group = group_state.snapshot()
        if source.site != group.site or source.source != group.source:
            raise RuntimeError('source identity conflicts with group bootstrap')
        if (
            source.stream_incarnation != group.verified_incarnation
            and 'stream-identity-changed' not in group.conditions
        ):
            raise RuntimeError('source incarnation conflicts with group bootstrap')

    def _overdue_group_health(
        self,
        worker: _WorkerHandle,
    ) -> tuple[GroupHealth, ...]:
        """Return lanes confirmed overdue by their accepted frame timestamp."""
        update = self._spawns[worker.spec.worker_id].health
        if update is None:
            return ()
        return tuple(
            group
            for group in update.groups
            if self._phase_confirmed_overdue(update.worker.heartbeat_at, group.deadline_at)
        )

    def _conditional_park_evidence(
        self,
        worker: _WorkerHandle,
    ) -> _ParkEvidence | None:
        """Read exact durable revision for one attributable overdue lane."""
        overdue_groups = self._overdue_group_health(worker)
        if len(overdue_groups) != 1:
            return None
        group = overdue_groups[0]
        update = self._spawns[worker.spec.worker_id].health
        reconciled = self._reconciled
        assignment = (
            None if reconciled is None else reconciled.get((worker.spec.site, group.group))
        )
        event_id = group.current_event_id
        path = self._group_source_path(worker.spec.site, group.group)
        if (
            update is None
            or not isinstance(assignment, ActiveSourceGroupAssignment)
            or group.phase != 'callback'
            or event_id is None
            or group.current_plugin_id is None
            or group.current_callback_id is None
            or group.delivery_attempt is None
            or not path.is_file()
        ):
            return None
        try:
            connection = _open_readonly_source(path)
            try:
                row = _source_store.park_evidence_state(connection, group.group)
            finally:
                connection.close()
        except sqlite3.Error:
            return None
        if (
            row is None
            or row[2] != event_id
            or bool(row[3])
            or type(row[0]) is not int
            or type(row[1]) is not int
            or row[0] != assignment.group_generation
        ):
            return None
        return _ParkEvidence(
            directive=ConditionalParkDirective(
                evidence_id=self._next_token(),
                site=worker.spec.site,
                group=group.group,
                group_generation=row[0],
                failed_spawn_instance=worker.spawn_instance,
                event_id=event_id,
                group_revision=row[1],
                observed_at_wall_clock=self._wall_clock(),
            ),
            failed_worker_id=worker.worker_id,
        )

    @staticmethod
    def _pending_recovery(
        assignment: WorkerAssignment,
    ) -> dict[str, ConditionalParkDirective]:
        """Index exact assigned recovery directives by evidence identity."""
        return {
            directive.evidence_id: directive
            for item in assignment.group_assignments
            if (directive := item.conditional_park) is not None
        }

    def _prepare_omissions(
        self,
        configured: Mapping[tuple[str, str], str],
        source_states: Mapping[tuple[str, str], SourceState],
        group_states: Mapping[tuple[str, str], GroupState],
    ) -> None:
        """Commit new removals while configured inventory still fences incomplete work."""
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        for (site, group), record in state.inventory_records().items():
            if not record.configured or (site, group) in configured:
                continue
            group_state = group_states.get((site, group))
            source_state = source_states.get((site, record.source))
            if group_state is None or source_state is None:
                raise RuntimeError(f'omitted group state is unavailable: {site}/{group}')
            snapshot = group_state.snapshot()
            if (
                snapshot.site != site
                or snapshot.group != group
                or snapshot.source != record.source
                or snapshot.group_generation not in {record.generation, record.orphan_generation}
            ):
                raise RuntimeError(f'omitted group authority conflicts: {site}/{group}')
            self._omitted_park_evidence(site, group, (record.generation, record.orphan_generation))
            self._check_source_incarnation(source_state, group_state)
            self._orphan_group(
                source_state,
                group_state,
                transition_generation=state.generation_high_water(site, group),
            )

    def _project_group_health(
        self,
        group: GroupHealth,
        watchdog: WatchdogHealth | None,
        heartbeat_at: float,
        now: float,
    ) -> GroupHealth:
        """Project one supervisor-authoritative lane watchdog."""
        if watchdog is None:
            return group
        deadline_at = watchdog.deadline_at
        return replace(
            group,
            phase=cast(
                'GroupPhase',
                watchdog.phase,
            ),
            current_event_id=watchdog.event_id,
            current_plugin_id=watchdog.plugin_id,
            current_callback_id=watchdog.callback_id,
            delivery_attempt=watchdog.delivery_attempt,
            heartbeat_at=watchdog.heartbeat_at,
            heartbeat_age=max(0.0, now - watchdog.heartbeat_at),
            deadline_at=deadline_at,
            overdue_reason=(
                'lane-phase-overdue'
                if self._phase_confirmed_overdue(heartbeat_at, deadline_at)
                else None
            ),
        )

    def _project_source_health(
        self,
        source: SourceHealth,
        watchdog: WatchdogHealth | None,
        heartbeat_at: float,
        now: float,
    ) -> SourceHealth:
        """Project one supervisor-authoritative source watchdog."""
        if watchdog is None:
            return source
        deadline_at = watchdog.deadline_at
        return replace(
            source,
            phase=cast(
                'SourceWatchdogPhase',
                watchdog.phase,
            ),
            heartbeat_at=watchdog.heartbeat_at,
            heartbeat_age=max(0.0, now - watchdog.heartbeat_at),
            deadline_at=deadline_at,
            overdue_reason=(
                'source-phase-overdue'
                if self._phase_confirmed_overdue(heartbeat_at, deadline_at)
                else None
            ),
        )

    def _promote_readded_groups(
        self,
        configured: Mapping[tuple[str, str], str],
        generations: Mapping[tuple[str, str], int],
        source_states: Mapping[tuple[str, str], SourceState],
        group_states: Mapping[tuple[str, str], GroupState],
    ) -> tuple[bool, dict[tuple[str, str], int]]:
        """Promote terminal re-adds and return current durable generations."""
        state = self._state
        if state is None:
            raise RuntimeError('supervisor is not open')
        promoted = False
        current_generations = dict(generations)
        for site_group, source_name in sorted(configured.items()):
            site_name, group_name = site_group
            group_state = group_states.get((site_name, group_name))
            source_state = source_states.get((site_name, source_name))
            if group_state is None and source_state is None:
                continue
            if group_state is None or source_state is None:
                raise RuntimeError(
                    f'durable promotion authority is incomplete: {site_name}/{group_name}'
                )
            current_generations[site_group] = group_state.snapshot().group_generation
            promoted = (
                state.promote_readded_group(
                    site=site_name,
                    group=group_name,
                    source_state=source_state,
                    group_state=group_state,
                )
                or promoted
            )
        return promoted, current_generations

    @staticmethod
    def _receive_operator_batch(
        worker: _WorkerHandle, first: ControlMessage
    ) -> Iterator[ControlMessage]:
        """Yield the first response before acquiring the remaining finite batch."""
        yield first
        yield from worker.receive_available()

    def _receive_operator_messages(
        self, worker: _WorkerHandle, messages: Iterator[ControlMessage]
    ) -> Iterator[ControlMessage]:
        """Normalize acquisition failures without reclassifying dispatch errors."""
        while True:
            try:
                with self._normalize_worker_control_errors(worker):
                    message = next(messages)
            except StopIteration:
                return
            yield message

    def _receive_operator_response(
        self,
        worker: _WorkerHandle,
        deadline: float,
    ) -> ControlMessage | None:
        """Receive one poll slice without treating an empty slice as a failure."""
        timeout = min(
            self._config.daemon.supervisor_poll_interval_secs,
            max(0.0, deadline - time.monotonic()),
        )
        try:
            with self._normalize_worker_control_errors(worker):
                message = worker.receive(timeout=timeout)
        except TimeoutError:
            return None
        self._advance_progress()
        return message

    def _receive_worker_message(self, worker: _WorkerHandle) -> ControlMessage:
        """Read the current owner's finite batch without reacquiring on handoff."""
        batches = self._message_batches
        if batches is None:
            return worker.receive(timeout=0.0)
        if worker.spawn_instance not in batches:
            batches[worker.spawn_instance] = worker.receive_available()
        try:
            message = next(batches[worker.spawn_instance])
        except StopIteration:
            raise TimeoutError('worker control batch exhausted') from None
        self._batch_message_count += 1
        return message

    def _reconcile_configured_authorities(
        self,
        configured: Mapping[tuple[str, str], str],
        reconciled: Mapping[tuple[str, str], SourceGroupAssignment],
        source_states: Mapping[tuple[str, str], SourceState],
        group_states: Mapping[tuple[str, str], GroupState],
    ) -> None:
        """Recover dispositions for every available configured authority."""
        for site_group, source_name in sorted(configured.items()):
            site_name, group_name = site_group
            source_state = source_states.get((site_name, source_name))
            group_state = group_states.get((site_name, group_name))
            if source_state is None and group_state is None:
                assignment = reconciled[site_group]
                if (
                    isinstance(assignment, ActiveSourceGroupAssignment)
                    and assignment.start_mode == 'initialize'
                ):
                    continue
                raise RuntimeError(
                    f'resume reconciliation state is unavailable: {site_name}/{group_name}'
                )
            if source_state is None or group_state is None:
                raise RuntimeError(
                    f'durable reconciliation authority is incomplete: {site_name}/{group_name}'
                )
            self._check_source_incarnation(source_state, group_state)
            assignment = reconciled[site_group]
            if (
                isinstance(assignment, PendingDispositionSourceGroupAssignment)
                and group_state.snapshot().group_generation == assignment.orphan_generation
                and group_state.lifecycle() == 'orphaned'
            ):
                group_state.change_lifecycle(
                    LifecycleRequest(
                        command='readd',
                        expected_lifecycle='orphaned',
                        expected_generation=assignment.orphan_generation,
                        transition_generation=assignment.reserved_generation,
                        identity=AuditIdentity.current(),
                        wall_clock_time=self._wall_clock(),
                    )
                )

    def _reconcile_omissions(
        self,
        source_states: Mapping[tuple[str, str], SourceState],
        group_states: Mapping[tuple[str, str], GroupState],
    ) -> None:
        """Collapse every omitted durable group through both authorities."""
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        records = state.inventory_records()
        for (site, group), generation in sorted(state.orphaned_groups().items()):
            group_state = group_states.get((site, group))
            if group_state is None:
                raise RuntimeError(f'omitted group state is unavailable: {site}/{group}')
            group_snapshot = group_state.snapshot()
            record = records[(site, group)]
            if (
                group_snapshot.site != site
                or group_snapshot.group != group
                or group_snapshot.group_generation not in {generation, record.orphan_generation}
            ):
                raise RuntimeError(f'omitted group authority conflicts: {site}/{group}')
            evidence = self._omitted_park_evidence(
                site, group, (generation, record.orphan_generation)
            )
            source_state = source_states.get((site, group_snapshot.source))
            if source_state is None:
                raise RuntimeError(
                    f'omitted group source state is unavailable: {site}/{group_snapshot.source}'
                )
            self._check_source_incarnation(source_state, group_state)
            if (
                record.orphan_generation is not None
                and group_snapshot.group_generation == generation
            ):
                if not self._current_disposition_proof(source_state, group_state, generation):
                    raise RuntimeError(f'omitted group terminal is unavailable: {site}/{group}')
                state.complete_orphan_disposition(
                    site=site,
                    group=group,
                    orphan_generation=record.orphan_generation,
                    reserved_generation=generation,
                )
            elif not self._completed_absent_disposition(record, source_state, group_state):
                self._orphan_group(
                    source_state,
                    group_state,
                    transition_generation=state.generation_high_water(site, group),
                )
            if evidence is None:
                continue
            state.retire_park_evidence(evidence.directive.evidence_id)
            self._park_evidence_by_group.pop((site, group))

    def _reconciliation_source_metadata(
        self,
        *,
        site: str,
        source: str,
    ) -> _ReconciliationSourceMetadata:
        """Read one source's durable process generation."""
        path = source_database_path(self._config, site, source)
        connection = _open_readonly_source(path)
        try:
            try:
                generation = _source_store.meta(connection)[7]
            except RuntimeError as error:
                raise SourceIdentityError(
                    f'{path}: source database metadata is missing (source-database-uninitialized)',
                ) from error
        finally:
            connection.close()
        return _ReconciliationSourceMetadata(
            source_generation=generation,
        )

    def _refresh_component_health(self, update: HealthUpdate, now: float) -> HealthUpdate:
        """Project current public ages and frame-confirmed phase expiry."""
        by_source = {
            item.source: item
            for item in update.watchdogs
            if item.kind == 'source' and item.source is not None
        }
        by_group = {
            item.group: item
            for item in update.watchdogs
            if item.kind == 'lane' and item.group is not None
        }
        return replace(
            update,
            sources=tuple(
                self._project_source_health(
                    source, by_source.get(source.source), update.worker.heartbeat_at, now
                )
                for source in update.sources
            ),
            groups=tuple(
                self._project_group_health(
                    group, by_group.get(group.group), update.worker.heartbeat_at, now
                )
                for group in update.groups
            ),
        )

    def _probe_snapshot(self, now: float) -> ProbeSnapshot:
        """Build one cheap complete projection from current in-memory state."""
        updates = tuple(
            self._refresh_component_health(update, now)
            for spawn in self._spawns.values()
            if (update := spawn.health) is not None
        )
        sources = tuple(
            {
                (source.site, source.source): source
                for update in updates
                for source in update.sources
            }.values()
        )
        groups = tuple(
            {
                (group.site, group.group): group for update in updates for group in update.groups
            }.values()
        )
        workers = tuple(update.worker for update in updates)
        expected_worker_ids = {
            spec.worker_id
            for site in self._config.sites.values()
            for spec in build_worker_specs(site)
        }
        expected_sources = {
            (site.name, source.name)
            for site in self._config.sites.values()
            for source in site.sources.values()
        }
        expected_groups = {
            (site.name, group.name)
            for site in self._config.sites.values()
            for group in site.groups.values()
        }
        complete = (
            expected_worker_ids == {worker.worker_id for worker in workers}
            and all(
                self._spawns[worker_id].health_inventory_complete
                for worker_id in expected_worker_ids
            )
            and expected_sources == {(source.site, source.source) for source in sources}
            and expected_groups == {(group.site, group.group) for group in groups}
        )
        stale_after_secs = self._config.daemon.health_interval_secs * _WORKER_STALE_INTERVALS
        worker_breaker_open = any(breaker.is_open for breaker in self._breakers.values())
        evaluation = evaluate_health(
            sources=sources,
            groups=groups,
            workers=workers,
            now=now,
            stale_after_secs=stale_after_secs,
            worker_breaker_open=worker_breaker_open,
            inventory_complete=complete,
            supervisor_conditions=self._state.conditions() if self._state is not None else (),
        )
        return ProbeSnapshot(
            generated_at=now,
            ready=evaluation.readiness,
            complete=complete,
            conditions=evaluation.conditions,
            shutting_down=self._shutting_down,
            halted=self._halt_reason_value is not None,
        )

    def _register_spawn(
        self,
        spec: WorkerSpec,
        spawn_instance: str,
        assignment: WorkerAssignment,
    ) -> _WorkerHandle:
        """Create and register one worker's startup lifecycle."""
        worker = self._worker_factory(
            config_path=self._config_path,
            spec=spec,
            spawn_instance=spawn_instance,
            max_frame_bytes=self._config.daemon.max_ipc_frame_bytes,
            send_deadline_secs=2 * self._config.daemon.health_interval_secs,
            startup_deadline_secs=self._config.daemon.worker_startup_deadline_secs,
            shutdown_deadline_secs=self._config.daemon.shutdown_deadline_secs,
            wait_slice_secs=self._config.daemon.supervisor_poll_interval_secs,
            foreground_progress=self._advance_progress,
        )
        self._restart_deadlines.pop(spec.worker_id, None)
        spawn = _WorkerSpawn(
            worker=worker,
            health_deadline=0.0,
            pending_groups=frozenset(),
            lifecycle=None,
        )
        # Own the child before fallible clock and recovery initialization.
        self._spawns[spec.worker_id] = spawn
        spawn.pending_groups = frozenset(
            item.group
            for item in assignment.source_group_assignments
            if isinstance(item, PendingDispositionSourceGroupAssignment)
            and item.source in spec.sources
        )
        started_at = self._clock()
        deadline = started_at + self._config.daemon.worker_startup_deadline_secs
        spawn.health_deadline = deadline
        spawn.pending_recovery = self._pending_recovery(assignment)
        spawn.lifecycle = _WorkerLifecycle(
            assignment=assignment,
            deadline_at=deadline,
            worker=worker,
            step_started_at=started_at,
        )
        return worker

    def _register_worker(self, worker: _WorkerHandle) -> None:
        """Register one ready spawn without replacing its breaker history."""
        spec = worker.spec
        breaker = self._ensure_worker_breaker(spec)
        breaker.mark_restart_started()
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        state.persist_worker_breaker(breaker)
        self._restart_deadlines.pop(spec.worker_id, None)

    def _restart_startup_clock(self, lifecycle: _WorkerLifecycle) -> None:
        """Start a fresh startup budget for the lifecycle and its initial health."""
        lifecycle.step_started_at = self._clock()
        deadline = lifecycle.step_started_at + self._config.daemon.worker_startup_deadline_secs
        lifecycle.deadline_at = deadline
        self._spawns[lifecycle.worker.worker_id].health_deadline = deadline

    def _restore_startup_breakers(self, should_stop: Callable[[], bool]) -> None:
        """Restore every breaker before the first child can run."""
        state = self._state
        if state is None:
            raise RuntimeError('supervisor is not open')
        with state.starting(should_stop):
            for site in self._config.sites.values():
                for spec in build_worker_specs(site):
                    self._ensure_worker_breaker(spec)
        if should_stop():
            raise WriteLockStoppedError('supervisor database startup stopped')

    def _retire_dependent_grant(self, worker: _WorkerHandle) -> None:
        """Release a retiring dependent's read ownership and fence its late reply."""
        lifecycle = self._spawns[worker.worker_id].lifecycle
        grant = None if lifecycle is None else lifecycle.active_grant()
        if lifecycle is not None and grant is not None:
            self._stale_grant_readies.add(
                EventPeerReady(
                    source=grant.source,
                    group=grant.group,
                    dependent_spawn_instance=grant.dependent_spawn_instance,
                )
            )
            lifecycle.grant_sent = False

    def _discard_worker_lifecycle(self, worker: _WorkerHandle) -> None:
        """Retire every ephemeral projection owned by one exact worker."""
        worker_id = worker.worker_id
        if (spawn := self._spawns.get(worker_id)) is None or spawn.worker is not worker:
            raise RuntimeError('worker lifecycle owner conflicts with current spawn')
        lifecycle = self._spawns[worker_id].lifecycle
        if lifecycle is not None and lifecycle.worker is not worker:
            raise RuntimeError('worker startup lifecycle owner conflicts with current spawn')
        self._retire_dependent_grant(worker)
        try:
            worker.close()
        finally:
            self._spawns.pop(worker_id)
            self._restart_deadlines.pop(worker_id, None)

    def _mark_current_worker_unready(
        self,
        worker: _WorkerHandle,
        reason: str,
        watchdog_failure: bool,
    ) -> bool:
        """Fence a stale spawn and update its cached health before termination."""
        if (
            (spawn := self._spawns.get(worker.worker_id)) is None
            or spawn.worker is not worker
            or self._spawns[worker.worker_id].termination_reason is not None
        ):
            return False
        self._retire_dependent_grant(worker)
        health = self._spawns[worker.worker_id].health
        if health is not None:
            self._spawns[worker.worker_id].health = replace(
                health,
                worker=replace(
                    health.worker,
                    monitor_alive=False,
                    watchdog_failure=(
                        reason if watchdog_failure else health.worker.watchdog_failure
                    ),
                ),
            )
        return True

    def _contain_current_worker(
        self,
        worker: _WorkerHandle,
        reason: str,
        *,
        watchdog_failure: bool = False,
    ) -> bool:
        """Apply bounded current-spawn health containment and park policy."""
        spawn = self._spawns.get(worker.worker_id)
        if spawn is None or spawn.worker is not worker or spawn.termination_reason is not None:
            return False
        if (
            reason == 'worker-heartbeat-overdue'
            and spawn.health_failure is None
            and spawn.lifecycle is None
            and spawn.health_inventory_complete
            and self._clock() >= spawn.health_deadline
        ):
            try:
                if worker.has_pending_input():
                    return False
            except (EOFError, OSError, ValueError):
                self._fail_current_worker(worker, 'worker-control-eof')
                return True
        if not self._mark_current_worker_unready(worker, reason, watchdog_failure):
            return False
        evidence = (
            self._conditional_park_evidence(worker) if reason == 'lane-phase-overdue' else None
        )
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        if evidence is not None and state.record_park_evidence(evidence):
            self._park_evidence_by_group[(evidence.directive.site, evidence.directive.group)] = (
                evidence
            )
            self._spawns[worker.worker_id].attributed_park_failure = True
        self._spawns[worker.worker_id].termination_reason = reason
        worker.kill()
        return True

    def _fail_current_worker(
        self,
        worker: _WorkerHandle,
        reason: str,
        *,
        watchdog_failure: bool = False,
    ) -> None:
        """Make one current spawn unready before terminating it."""
        if not self._mark_current_worker_unready(worker, reason, watchdog_failure):
            return
        self._spawns[worker.worker_id].termination_reason = reason
        worker.kill()

    def _release_source_ownership(self, worker: _WorkerHandle) -> None:
        """Release one exited source owner and stop its live dependents."""
        if not worker.spec.sources:
            return
        for key, owner in tuple(self._source_workers.items()):
            if owner is worker:
                self._source_workers.pop(key)
                self._source_endpoints.pop(key, None)
        for dependent_id, spawn in tuple(self._spawns.items()):
            dependent = spawn.worker
            if (
                dependent.spec.site == worker.spec.site
                and not dependent.spec.sources
                and self._spawns[dependent_id].termination_reason is None
            ):
                self._retire_dependent_grant(dependent)
                self._spawns[dependent_id].termination_reason = 'source-owner-unavailable'
                dependent.kill()

    def _runnable_worker_spec(self, spec: WorkerSpec) -> WorkerSpec:
        """Exclude pending dispositions from every worker's lane inventory."""
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        assignments = state.configured_group_assignments()
        groups = tuple(
            group
            for group in spec.groups
            if isinstance(assignments.get((spec.site, group)), ActiveSourceGroupAssignment)
        )
        return replace(spec, groups=groups)

    def _refresh_worker_spec(self, spec: WorkerSpec) -> WorkerSpec:
        """Rebuild this worker's configured lane candidates after a lifecycle change."""
        configured = next(
            item
            for item in build_worker_specs(self._config.sites[spec.site])
            if item.worker_id == spec.worker_id
        )
        current = self._runnable_worker_spec(configured)
        self._specs[current.worker_id] = current
        return current

    def _schedule_reaped_worker_restart(
        self,
        worker_id: str,
        reason: str,
        now: float,
        *,
        attributed: bool,
    ) -> None:
        """Persist one reaped worker outcome and any eligible restart."""
        breaker = self._breakers[worker_id]
        if attributed or reason == 'source-owner-unavailable':
            delay = 0.0
        else:
            delay = breaker.record_failure(reason)
        if delay is not None:
            self._restart_deadlines[worker_id] = (
                now + delay if breaker.restart_deadline_at is None else breaker.restart_deadline_at
            )
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        state.persist_worker_breaker(breaker)
        if delay is None:
            _LOGGER.warning(
                'worker %s exited (%s); breaker open; run reset-worker-breaker', worker_id, reason
            )
        else:
            _LOGGER.warning('worker %s exited (%s); restarting in %ss', worker_id, reason, delay)

    def _reap_workers(self, now: float) -> bool:
        """Nonblockingly reap exits and assign independent restart deadlines."""
        changed = False
        for worker_id, spawn in tuple(self._spawns.items()):
            worker = spawn.worker
            status = worker.wait(timeout=0.0)
            self._advance_progress()
            if status is None:
                continue
            reason = self._spawns[worker_id].termination_reason or f'worker-exit-{status}'
            attributed = self._spawns[worker_id].attributed_park_failure
            self._discard_worker_lifecycle(worker)
            self._release_source_ownership(worker)
            self._schedule_reaped_worker_restart(worker_id, reason, now, attributed=attributed)
            self._advance_progress()
            changed = True
        return changed

    def _shutdown_worker(
        self,
        worker: _WorkerHandle,
        reason: str,
    ) -> int:
        """Require one owned child to exit inside its forced reap bound."""
        status = worker.shutdown(reason)
        self._advance_progress()
        if status is None:
            self._halt_reason_value = f'worker child remained unreaped: {worker.worker_id}'
            self._progress.set_halted()
            raise RuntimeError(self._halt_reason_value)
        return status

    def _shutdown_workers(self, reason: str) -> None:
        """Stop every owned child in reverse startup order."""
        first_error: Exception | None = None
        first_halt_reason: str | None = None
        for spawn in reversed(tuple(self._spawns.values())):
            worker = spawn.worker
            try:
                self._shutdown_worker(worker, reason)
            except Exception as error:  # noqa: BLE001
                if first_error is None:
                    first_error = error
                    first_halt_reason = self._halt_reason_value
            try:
                self._discard_worker_lifecycle(worker)
            except Exception as error:  # noqa: BLE001
                if first_error is None:
                    first_error = error
                    first_halt_reason = self._halt_reason_value
        self._source_endpoints.clear()
        self._source_workers.clear()
        self._stale_grant_readies.clear()
        self._restart_deadlines.clear()
        if first_error is not None:
            self._halt_reason_value = first_halt_reason
            if first_halt_reason is not None:
                self._progress.set_halted()
            raise first_error

    def _close(self) -> None:
        """Stop every child and release resources despite cleanup failures."""
        self._shutting_down = True
        self._progress.set_shutting_down()
        self._advance_progress()

        def close_workers() -> None:
            try:
                self._shutdown_workers('supervisor-close')
                self._advance_progress()
            finally:
                self._adopting_sources.clear()
                self._breakers.clear()
                self._park_evidence_by_group.clear()
                self._pending_dependents.clear()
                self._pending_disposition_resumes.clear()
                self._reconciled = None
                self._restart_deadlines.clear()
                self._specs.clear()

        def close_state() -> None:
            state = self._state
            try:
                if state is not None:
                    state.close()
            finally:
                self._state = None
                self._next_checkpoint_at = None

        def close_lock() -> None:
            lock = self._lock
            try:
                if lock is not None:
                    lock.close()
            finally:
                self._lock = None

        close_callables((close_workers, close_state, close_lock))

    def _shutdown_after_failure(self, reason: str, error: BaseException) -> None:
        """Preserve one active failure while attempting worker shutdown."""
        halt_reason = self._halt_reason_value
        try:
            self._shutdown_workers(reason)
        except Exception as cleanup_error:  # noqa: BLE001
            error.add_note(str(cleanup_error))
        finally:
            self._halt_reason_value = halt_reason
            if halt_reason is not None:
                self._progress.set_halted()

    def _source_adoption_restart_ids(
        self,
        site: str,
        source: str,
        groups: Collection[str],
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Select source owners and their endpoint-bound dependents."""
        source_worker_ids = tuple(
            worker.worker_id
            for spawn in self._spawns.values()
            for worker in (spawn.worker,)
            if worker.spec.site == site and source in worker.spec.sources
        )
        source_worker_sources = {
            source for worker_id in source_worker_ids for source in self._specs[worker_id].sources
        }
        dependent_groups = set(groups)
        dependent_groups.update(
            group.name
            for group in self._config.sites[site].groups.values()
            if group.source in source_worker_sources
        )
        dependent_ids = tuple(
            worker.worker_id
            for spawn in self._spawns.values()
            for worker in (spawn.worker,)
            if worker.spec.site == site
            and not worker.spec.sources
            and any(
                group in dependent_groups
                for configured_spec in build_worker_specs(self._config.sites[site])
                if configured_spec.worker_id == worker.worker_id
                for group in configured_spec.groups
            )
        )
        return source_worker_ids, dependent_ids

    def _source_adoption_target(
        self,
        command: OperatorCommand,
    ) -> tuple[str, str, str]:
        """Validate and return one configured source-adoption target."""
        if (
            command.command != 'adopt-source-stream'
            or frozenset(command.arguments) != SOURCE_ADOPTION_ARGUMENTS
        ):
            raise ValueError('source adoption command is invalid')
        site = command.arguments['site']
        source = command.arguments['source']
        reason = command.arguments['reason']
        if not isinstance(site, str) or site not in self._config.sites:
            raise ValueError(f'source adoption site {site!r} is not configured; check --site')
        if not isinstance(source, str) or source not in self._config.sites[site].sources:
            raise ValueError(
                f'source adoption source {site}/{source} is not configured; check --source'
            )
        if not isinstance(reason, str) or not reason:
            raise ValueError('source adoption reason must be nonempty text')
        return site, source, reason

    def _source_writes(self, site: str, source: str) -> WriteContext:
        """Own one bounded source-file writer without worker watch or stop state."""
        lock = SourceWriteLock(
            source_write_lock_path(self._config, site, source),
            database_path=source_database_path(self._config, site, source),
            timeout_secs=OPERATOR_WRITE_BUDGET_SECS,
            wait_phase_grace_secs=watchdog_grace_secs(self._config.daemon.health_interval_secs),
        )
        return WriteContext(lock, policy='operator')

    def _open_lifecycle_group(
        self,
        metadata: _OrphanGroupMetadata,
        *,
        group: str,
        operator: bool = False,
    ) -> GroupState:
        """Open one configured or absent group with stored or freshly observed source facts."""
        site = self._config.sites[metadata.site]
        source_config = site.sources[metadata.source]
        configured = site.groups.get(group)
        site_server_url = site.server
        source_generation = metadata.source_generation
        configured_fingerprint = source_stream_fingerprint(site, source_config)
        verified_incarnation = metadata.verified_incarnation
        opener = GroupState.open_for_disposition if operator else GroupState.open
        return opener(
            source_database_path(self._config, metadata.site, metadata.source),
            group_lock_path(self._config, metadata.site, group),
            writes=(
                self._source_writes(metadata.site, metadata.source).for_operator()
                if operator
                else self._source_writes(metadata.site, metadata.source)
            ),
            **group_state_options(self._config.daemon, source_config, configured),
            site=metadata.site,
            site_server_url=site_server_url,
            source=metadata.source,
            group=group,
            group_generation=metadata.group_generation,
            ordering=metadata.ordering if configured is None else configured.ordering,
            stream_fingerprint=configured_fingerprint,
            source_generation=source_generation,
            verified_incarnation=verified_incarnation,
            start_mode='resume',
        )

    def _open_reconciliation_groups(
        self,
        stack: ExitStack,
        identities: Collection[tuple[str, str]],
        configured: Mapping[tuple[str, str], str],
        source_states: Mapping[tuple[str, str], SourceState],
    ) -> tuple[
        dict[tuple[str, str], int],
        dict[tuple[str, str], GroupState],
    ]:
        """Open every existing group authority needed for reconciliation."""
        generations: dict[tuple[str, str], int] = {}
        states: dict[tuple[str, str], GroupState] = {}
        for site_name, group_name in sorted(identities):
            path = self._group_source_path(site_name, group_name)
            if not self._group_state_exists(path, group_name):
                continue
            metadata = self._orphan_group_metadata(
                site=site_name,
                group=group_name,
            )
            generations[(site_name, group_name)] = metadata.group_generation
            configured_source = configured.get((site_name, group_name))
            if configured_source is None:
                states[(site_name, group_name)] = stack.enter_context(
                    self._open_lifecycle_group(metadata, group=group_name)
                )
                continue
            site = self._config.sites[site_name]
            group_config = site.groups[group_name]
            source_config = site.sources[configured_source]
            source_state = source_states.get((site_name, configured_source))
            if source_state is None:
                raise RuntimeError(
                    f'durable group source state is unavailable: {site_name}/{group_name}'
                )
            source_snapshot = source_state.snapshot()
            incarnation = source_snapshot.stream_incarnation
            if incarnation is None:
                raise RuntimeError(
                    f'durable group source incarnation is unavailable: {site_name}/{group_name}'
                )
            states[(site_name, group_name)] = stack.enter_context(
                GroupState.open(
                    path,
                    group_lock_path(self._config, site_name, group_name),
                    writes=self._source_writes(site_name, configured_source),
                    **group_state_options(self._config.daemon, source_config, group_config),
                    site=site_name,
                    site_server_url=site.server,
                    source=configured_source,
                    group=group_name,
                    group_generation=metadata.group_generation,
                    ordering=group_config.ordering,
                    stream_fingerprint=source_stream_fingerprint(site, source_config),
                    source_generation=source_snapshot.source_generation,
                    verified_incarnation=incarnation,
                    start_mode='resume',
                )
            )
        return generations, states

    def _open_reconciliation_sources(
        self,
        stack: ExitStack,
        identities: Collection[tuple[str, str]],
    ) -> dict[tuple[str, str], SourceState]:
        """Open every existing source authority needed for reconciliation."""
        states: dict[tuple[str, str], SourceState] = {}
        for site_name, source_name in sorted(identities):
            path = source_database_path(self._config, site_name, source_name)
            if not path.is_file():
                continue
            site = self._config.sites.get(site_name)
            if site is None or source_name not in site.sources:
                raise RuntimeError(
                    f'durable source is no longer configured: {site_name}/{source_name}'
                )
            source_config = site.sources[source_name]
            metadata = self._reconciliation_source_metadata(
                site=site_name,
                source=source_name,
            )
            states[(site_name, source_name)] = stack.enter_context(
                SourceState.open(
                    path,
                    writes=self._source_writes(site_name, source_name),
                    **source_state_options(self._config.daemon, source_config),
                    site=site_name,
                    source=source_name,
                    stream_fingerprint=source_stream_fingerprint(site, source_config),
                    source_generation=metadata.source_generation,
                )
            )
        return states

    def _run_direct_disposition(
        self, command: OperatorCommand, metadata: _OrphanGroupMetadata, request: DispositionRequest
    ) -> CommandResult:
        """Execute only while both source and group ownership locks are available."""
        path = source_database_path(self._config, metadata.site, metadata.source)
        try:
            source_lock = acquire_lock(source_lock_path(path))
        except AlreadyLockedError as error:
            return CommandResult(
                command=command.command,
                ok=False,
                message=str(error),
                request_id=command.request_id,
                result={'code': SOURCE_BUSY_CODE},
            )
        with source_lock:
            try:
                with self._open_lifecycle_group(
                    metadata, group=request.group, operator=True
                ) as group_state:
                    result = group_state.dispose(request)
            except AlreadyLockedError as error:
                return CommandResult(
                    command=command.command,
                    ok=False,
                    message=str(error),
                    request_id=command.request_id,
                    result={'code': GROUP_BUSY_CODE},
                )
        fields = group_result_mapping(result)
        return CommandResult(
            command=command.command,
            ok=True,
            message='accepted',
            request_id=command.request_id,
            result={
                name: fields[name]
                for name in (
                    'conditions',
                    'cursor',
                    'group',
                    'group_generation',
                    'parked',
                    'revision',
                    'site',
                    'source',
                    'status',
                )
            },
        )

    def _spawn_dependent(
        self,
        site_name: str,
        spec: WorkerSpec,
    ) -> _WorkerHandle:
        """Begin one dependent startup without waiting for authentication."""
        spec = self._refresh_worker_spec(spec)
        site = self._config.sites[site_name]
        breaker = self._ensure_worker_breaker(spec)
        if breaker.is_open:
            raise RuntimeError(f'worker breaker is open: {spec.worker_id}')
        dependent_spawn = self._next_token()
        grants = tuple(
            EventPeerGrant(
                source=site.groups[group_name].source,
                group=site.groups[group_name].name,
                dependent_spawn_instance=dependent_spawn,
                capability=self._next_token(),
            )
            for group_name in spec.groups
        )
        dependent_sources = tuple(
            self._source_endpoints[(site_name, source)]
            for source in sorted({grant.source for grant in grants})
        )
        assignment = self._build_worker_assignment(
            spec,
            dependent_spawn,
            (),
            dependent_sources,
            grants,
        )
        return self._register_spawn(spec, dependent_spawn, assignment)

    def _spawn_source(
        self,
        spec: WorkerSpec,
    ) -> _WorkerHandle:
        """Begin one source-owner startup without waiting for readiness."""
        spec = self._refresh_worker_spec(spec)
        if self._reconciled is None:
            raise RuntimeError('supervisor state is not reconciled')
        breaker = self._ensure_worker_breaker(spec)
        if breaker.is_open:
            raise RuntimeError(f'worker breaker is open: {spec.worker_id}')
        source_generations = {source: self._next_token() for source in spec.sources}
        source_spawn = self._next_token()
        assignment = self._build_worker_assignment(
            spec,
            source_spawn,
            tuple(sorted(source_generations.items())),
            (),
            (),
        )
        return self._register_spawn(spec, source_spawn, assignment)

    def _restart_due_workers(self, now: float) -> bool:
        """Restart every independently due worker in source-before-dependent order."""
        due = sorted(
            (
                self._specs[worker_id]
                for worker_id, deadline in self._restart_deadlines.items()
                if deadline <= now and worker_id not in self._spawns
            ),
            key=lambda spec: (not bool(spec.sources), spec.site, spec.worker_id),
        )
        restarted = False
        for previous_spec in due:
            spec = self._refresh_worker_spec(previous_spec)
            if not spec.sources and not self._dependent_sources_ready(spec):
                continue
            if spec.sources:
                self._spawn_source(spec)
            else:
                self._spawn_dependent(spec.site, spec)
            restarted = True
            self._advance_progress()
        return restarted

    def _start_ready_dependents(self, site_name: str) -> bool:
        """Spawn configured dependents whose source owners are ready."""
        started = False
        for worker_id, spec in tuple(self._pending_dependents.items()):
            if spec.site != site_name:
                continue
            breaker = self._ensure_worker_breaker(spec)
            if breaker.is_open:
                self._pending_dependents.pop(worker_id)
                continue
            if (
                breaker.restart_deadline_at is not None
                and breaker.restart_deadline_at > self._clock()
            ):
                self._restart_deadlines[worker_id] = breaker.restart_deadline_at
                self._pending_dependents.pop(worker_id)
                continue
            if not self._dependent_sources_ready(spec):
                continue
            self._pending_dependents.pop(worker_id)
            self._spawn_dependent(site_name, spec)
            started = True
        return started

    def _startup_clock(self, spawn: _WorkerSpawn) -> tuple[float, float] | None:
        """Return the current own-step clock until readiness and complete health."""
        lifecycle = spawn.lifecycle
        if lifecycle is not None:
            if self._lifecycle_unclocked(lifecycle):
                return None
            return lifecycle.step_started_at, lifecycle.deadline_at
        if spawn.health_inventory_complete:
            return None
        # Initial health keeps the assignment clock after endpoint readiness.
        deadline = spawn.health_deadline
        return deadline - self._config.daemon.worker_startup_deadline_secs, deadline

    def _startup_clock_expired(self, spawn: _WorkerSpawn, now: float) -> bool:
        """Judge a passed own-step clock only from accepted worker evidence."""
        clock = self._startup_clock(spawn)
        if clock is None:
            return False
        started_at, deadline = clock
        if now < deadline:
            return False
        arrival = spawn.worker.last_arrival_at
        if arrival is None or arrival < started_at:
            return True
        if now >= arrival + _WORKER_STALE_INTERVALS * self._config.daemon.health_interval_secs:
            return True
        health = spawn.health
        return health is not None and health.worker.heartbeat_at >= deadline

    def _trusted_internal_watchdog_interval(self, watchdog: WatchdogHealth, source: str) -> float:
        """Keep operator waits inside the bound without extending other internal work."""
        budget = self._internal_watchdog_budget(watchdog.site, source)
        if watchdog.kind == 'operator' and watchdog.phase == 'internal':
            interval = self._config.daemon.health_interval_secs
            return max(budget, interval + watchdog_grace_secs(interval))
        return budget

    @staticmethod
    def _trusted_source_wait_delay(source: SourceConfig, detail: SourceHealth) -> float:
        """Clamp one retry wait or return the configured ordinary wait."""
        retrying = (
            detail.retry_attempt > 0
            or detail.next_retry_secs is not None
            or detail.last_error is not None
        )
        if not retrying:
            return source.fetch_interval_secs
        delay = detail.next_retry_secs
        if delay is None:
            raise ValueError('source retry wait is invalid')
        if type(delay) not in {float, int} or not isfinite(delay) or delay < 0:
            raise ValueError('source retry wait is invalid')
        retry_wait_cap = source.poll_retry_max_secs * (1 + source.poll_retry_jitter_fraction)
        return min(float(delay), retry_wait_cap)

    def _trusted_write_watchdog_interval(self, phase: _write_lock.WritePhase) -> float:
        """Return a write phase budget independent of provider call time."""
        daemon = self._config.daemon
        if phase == 'write-hold':
            return _write_lock.WRITE_HOLD_DEADLINE_SECS
        return _write_lock.write_lock_wait_budget_secs(
            _write_lock.write_lock_timeout_secs(
                daemon.health_interval_secs,
                daemon.supervisor_poll_interval_secs,
            ),
            watchdog_grace_secs(daemon.health_interval_secs),
        )

    def _trusted_watchdog_interval(
        self,
        watchdog: WatchdogHealth,
        sources: Mapping[str, SourceHealth],
    ) -> float:
        """Derive a phase budget from configured limits and bounded source retry timing."""
        site = self._config.sites[watchdog.site]
        daemon = self._config.daemon
        source_name = watchdog.source
        if watchdog.kind == 'lane':
            group_name = watchdog.group
            if group_name is None:
                raise ValueError('lane watchdog group is unavailable')
            group = site.groups[group_name]
            source_name = group.source
            if watchdog.phase in {'callback', 'setup'}:
                return group.callback_deadline_secs
        if watchdog.phase == 'shutdown':
            return daemon.shutdown_deadline_secs
        if watchdog.phase in {'write-hold', 'write-lock-wait'}:
            return self._trusted_write_watchdog_interval(
                cast('_write_lock.WritePhase', watchdog.phase)
            )
        if source_name is None:
            raise ValueError('watchdog source is unavailable')
        source = site.sources[source_name]
        if watchdog.kind == 'lane' and watchdog.phase == 'idle':
            retry_wait_cap = source.poll_retry_max_secs * (1 + source.poll_retry_jitter_fraction)
            return max(source.fetch_interval_secs, retry_wait_cap) + watchdog_grace_secs(
                daemon.health_interval_secs
            )
        if watchdog.kind != 'source' or watchdog.phase != 'wait':
            return self._trusted_internal_watchdog_interval(watchdog, source_name)
        detail = sources.get(source_name)
        if detail is None:
            raise ValueError('source wait detail is unavailable')
        return self._trusted_source_wait_delay(source, detail) + watchdog_grace_secs(
            daemon.health_interval_secs
        )

    def _trusted_watchdog_deadline(
        self,
        watchdog: WatchdogHealth,
        sources: Mapping[str, SourceHealth],
    ) -> float | None:
        """Return one supervisor-derived effective component deadline."""
        if watchdog.kind == 'operator' and watchdog.phase == 'idle':
            return None
        child_deadline = watchdog.deadline_at
        if (
            child_deadline is None
            or type(child_deadline) not in {float, int}
            or not isfinite(child_deadline)
            or child_deadline < 0
        ):
            raise ValueError('active watchdog deadline is invalid')
        interval = self._trusted_watchdog_interval(watchdog, sources)
        trusted_maximum = watchdog.heartbeat_at + interval
        return min(float(child_deadline), trusted_maximum)

    def _validate_operator_watchdog(
        self,
        worker: _WorkerHandle,
        watchdog: WatchdogHealth,
    ) -> None:
        """Validate one idle or exactly owner-routed operator cell."""
        site = self._config.sites[worker.spec.site]
        if watchdog.phase == 'idle':
            if watchdog.source is not None or watchdog.group is not None:
                raise ValueError('idle operator watchdog has an active target')
        elif watchdog.phase == 'shutdown' and watchdog.source is None:
            if watchdog.group is not None:
                raise ValueError('targetless operator shutdown has a group target')
        elif watchdog.group is not None:
            if (
                watchdog.group not in worker.spec.groups
                or watchdog.source != site.groups[watchdog.group].source
            ):
                raise ValueError('operator watchdog target conflicts with assignment')
        elif watchdog.source not in worker.spec.sources:
            raise ValueError('operator watchdog target conflicts with assignment')

    def _add_watchdog_to_inventory(
        self,
        worker: _WorkerHandle,
        watchdog: WatchdogHealth,
        source_watchdogs: dict[str, WatchdogHealth],
        lane_watchdogs: dict[str, WatchdogHealth],
    ) -> bool:
        """Validate and add one current configured watchdog owner."""
        site = self._config.sites[worker.spec.site]
        if watchdog.site != worker.spec.site:
            raise ValueError('worker watchdog site conflicts with current assignment')
        if watchdog.kind == 'source':
            source = watchdog.source
            if (
                source is None
                or source not in worker.spec.sources
                or watchdog.group is not None
                or source in source_watchdogs
            ):
                raise ValueError('source watchdog conflicts with current assignment')
            source_watchdogs[source] = watchdog
            return False
        if watchdog.kind == 'lane':
            group = watchdog.group
            if (
                group is None
                or group not in worker.spec.groups
                or watchdog.source != site.groups[group].source
                or group in lane_watchdogs
            ):
                raise ValueError('lane watchdog conflicts with current assignment')
            lane_watchdogs[group] = watchdog
            return False
        self._validate_operator_watchdog(worker, watchdog)
        return True

    def _validated_health_inventory(
        self,
        worker: _WorkerHandle,
        message: HealthUpdate,
    ) -> _HealthInventory:
        """Validate current configured source, lane, and operator inventory."""
        site = self._config.sites[worker.spec.site]
        source_records = {source.source: source for source in message.sources}
        group_records = {group.group: group for group in message.groups}
        expected_groups = set(worker.spec.groups) | self._pending_worker_groups(worker)
        if len(source_records) != len(message.sources) or any(
            source.site != worker.spec.site or source.source not in worker.spec.sources
            for source in message.sources
        ):
            raise ValueError('worker source inventory conflicts with current assignment')
        if len(group_records) != len(message.groups) or any(
            group.site != worker.spec.site
            or group.group not in expected_groups
            or site.groups[group.group].source != group.source
            for group in message.groups
        ):
            raise ValueError('worker group inventory conflicts with current assignment')
        source_watchdogs: dict[str, WatchdogHealth] = {}
        lane_watchdogs: dict[str, WatchdogHealth] = {}
        operator_count = sum(
            self._add_watchdog_to_inventory(
                worker,
                watchdog,
                source_watchdogs,
                lane_watchdogs,
            )
            for watchdog in message.watchdogs
        )
        if operator_count > 1:
            raise ValueError('operator watchdog conflicts with current assignment')
        return _HealthInventory(
            source_records=source_records,
            group_records=group_records,
            source_watchdogs=source_watchdogs,
            lane_watchdogs=lane_watchdogs,
            operator_count=operator_count,
        )

    def _wait_for_worker_locks(self) -> None:
        """Wait a bounded takeover interval for old worker state locks."""
        deadline = time.monotonic() + self._config.daemon.worker_startup_deadline_secs
        blocked = self._existing_worker_lock_paths()
        while blocked:
            still_blocked: list[tuple[Path, bool]] = []
            for path, is_write_lock in blocked:
                if is_write_lock:
                    fd, _ = secure_open_regular_file(path, create=False)
                    try:
                        available = try_flock_exclusive(fd)
                        if available:
                            release_flock(fd)
                    finally:
                        os.close(fd)
                    if not available:
                        still_blocked.append((path, True))
                else:
                    try:
                        probe = acquire_lock(path)
                    except AlreadyLockedError:
                        still_blocked.append((path, False))
                    else:
                        probe.close()
            if not still_blocked:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                paths_text = ', '.join(os.fspath(path) for path, _ in still_blocked)
                raise TimeoutError(f'worker locks did not clear before takeover: {paths_text}')
            time.sleep(
                min(
                    remaining,
                    self._config.daemon.supervisor_poll_interval_secs,
                )
            )
            blocked = still_blocked

    def _open(self) -> None:
        """Acquire the owner-only state-dir supervisor lock."""
        if self._lock is not None:
            return
        state_dir = ensure_secure_state_dir(self._config.daemon.state_dir)
        lock = acquire_lock(state_dir / 'supervisor.lock')
        state: _SupervisorState | None = None
        try:
            self._wait_for_worker_locks()
            state = _SupervisorState.open(
                state_dir,
                synchronous=self._config.daemon.sqlite_synchronous,
                interval_secs=self._config.daemon.checkpoint_interval_secs,
                clock=self._clock,
                checkpointer_factory=self._checkpointer_factory,
            )
            pending_evidence = state.pending_park_evidence()
        except BaseException as error:
            close_resources((state, lock), primary_error=error)
            raise
        self._lock = lock
        self._state = state
        self._park_evidence_by_group = {
            (evidence.directive.site, evidence.directive.group): evidence
            for evidence in pending_evidence
        }
        now = self._clock()
        if not isfinite(now) or now < 0:
            self._close()
            raise ValueError('supervisor clock must be finite and nonnegative')
        self._next_checkpoint_at = now + self._config.daemon.checkpoint_interval_secs

    @staticmethod
    def _watchdog_overdue_reason(watchdog: WatchdogHealth) -> str:
        """Return the bounded reason owned by one watchdog kind."""
        return {
            'source': 'source-phase-overdue',
            'lane': 'lane-phase-overdue',
            'operator': 'operator-phase-overdue',
        }[watchdog.kind]

    def _effective_watchdogs(
        self,
        watchdogs: tuple[WatchdogHealth, ...],
        sources: Mapping[str, SourceHealth],
        now: float,
    ) -> tuple[tuple[WatchdogHealth, ...], str | None]:
        """Clamp child projections to supervisor-derived phase deadlines."""
        effective: list[WatchdogHealth] = []
        invalid_reason: str | None = None
        for watchdog in watchdogs:
            observed = watchdog
            if watchdog.heartbeat_at > now:
                observed = replace(watchdog, heartbeat_at=now)
                deadline_at = now
                invalid_reason = self._watchdog_overdue_reason(watchdog)
            else:
                try:
                    deadline_at = self._trusted_watchdog_deadline(observed, sources)
                except (KeyError, ValueError):
                    deadline_at = now
                    invalid_reason = self._watchdog_overdue_reason(watchdog)
            effective.append(replace(observed, deadline_at=deadline_at))
        return tuple(effective), invalid_reason

    def _record_worker_health(
        self,
        worker: _WorkerHandle,
        message: HealthUpdate,
        now: float,
    ) -> bool:
        """Record one validated spawn-matched worker health update."""
        worker_id = worker.worker_id
        health = message.worker
        if health.spawn_instance != worker.spawn_instance or any(
            item.worker_instance != worker.spawn_instance
            for item in (*message.watchdogs, *message.sources, *message.groups)
        ):
            return False
        if health.site != worker.spec.site or health.worker_id != worker_id:
            raise ValueError('worker health identity conflicts with current spawn')
        inventory = self._validated_health_inventory(worker, message)
        effective, invalid_reason = self._effective_watchdogs(
            message.watchdogs,
            inventory.source_records,
            now,
        )
        current_worker = replace(
            health,
            heartbeat_at=min(now, health.heartbeat_at),
            heartbeat_age=max(0.0, now - min(now, health.heartbeat_at)),
        )
        previous = self._spawns[worker_id].health
        if previous is not None and any(
            group.group not in inventory.group_records for group in previous.groups
        ):
            invalid_reason = invalid_reason or 'lane-phase-overdue'
        self._spawns[worker_id].health = self._refresh_component_health(
            replace(message, worker=current_worker, watchdogs=effective),
            now,
        )
        inventory_reason = self._inventory_failure_reason(worker, inventory)
        complete = invalid_reason is None and inventory_reason is None
        if health.watchdog_failure is not None:
            invalid_reason = 'watchdog-sampling-failed'
        elif self._spawns[worker_id].health_inventory_complete and not complete:
            invalid_reason = invalid_reason or inventory_reason
        if invalid_reason is not None or not health.monitor_alive:
            self._spawns[worker_id].health_failure = invalid_reason or 'worker-heartbeat-overdue'
            self._spawns[worker_id].health_deadline = now
        elif complete:
            self._spawns[worker_id].health_inventory_complete = True
            self._spawns[worker_id].health_failure = None
            self._spawns[worker_id].health_deadline = (
                worker.last_arrival_at
                if worker.last_arrival_at is not None
                else current_worker.heartbeat_at
            ) + self._config.daemon.health_interval_secs * _WORKER_STALE_INTERVALS
        return True

    @staticmethod
    def _worker_control_failure_reason(failure: _WorkerControlError) -> str:
        """Return one bounded reason for an identified channel failure."""
        if failure.sending:
            if isinstance(failure.error, TimeoutError):
                return 'worker-control-send-timeout'
            return 'worker-control-send-failed'
        return 'worker-control-eof'

    def _contain_worker_control_failure(self, failure: _WorkerControlError) -> None:
        """Contain the exact current worker that authored a control failure."""
        self._fail_current_worker(
            failure.worker,
            self._worker_control_failure_reason(failure),
        )

    def _reconcile(
        self,
        *,
        generations: Mapping[tuple[str, str], int],
        source_states: Mapping[tuple[str, str], SourceState] | None = None,
        group_states: Mapping[tuple[str, str], GroupState] | None = None,
    ) -> None:
        """Reconcile durable group generations before spawning workers."""
        if self._spawns:
            raise RuntimeError('startup reconciliation cannot run after worker spawn')
        state = self._state
        if state is None:
            raise RuntimeError('supervisor is not open')
        if (source_states is None) != (group_states is None):
            raise ValueError('disposition reconciliation state mappings must be complete')
        configured = {
            (site.name, group.name): group.source
            for site in self._config.sites.values()
            for group in site.groups.values()
        }
        if source_states is not None and group_states is not None:
            self._prepare_omissions(configured, source_states, group_states)
        reconciled = state.reconcile_groups(
            configured=configured,
            generations=generations,
        )
        if source_states is not None and group_states is not None:
            self._reconcile_omissions(source_states, group_states)
            self._reconcile_configured_authorities(
                configured,
                reconciled,
                source_states,
                group_states,
            )
            promoted, current_group_generations = self._promote_readded_groups(
                configured,
                generations,
                source_states,
                group_states,
            )
            if promoted:
                reconciled = state.reconcile_groups(
                    configured=configured,
                    generations=current_group_generations,
                )
        self._reconciled = reconciled

    def _send_operator_command(self, worker: _WorkerHandle, command: OperatorCommand) -> None:
        """Send a command or contain its owner and report the uncertain outcome."""
        try:
            with self._normalize_worker_control_errors(worker, sending=True):
                worker.send(command)
        except _WorkerControlError as failure:
            self._contain_worker_control_failure(failure)
            raise OperatorOutcomeUnknownError(
                _control.unknown_outcome_message(command.command, 'the command could not be sent')
            ) from failure

    def _worker_health_failure(
        self,
        worker_id: str,
        health: HealthUpdate | None,
        now: float,
    ) -> str | None:
        """Return the first current bounded worker containment reason."""
        reason = self._spawns[worker_id].health_failure
        if reason is not None:
            return reason
        if health is not None:
            for watchdog in health.watchdogs:
                if self._phase_confirmed_overdue(health.worker.heartbeat_at, watchdog.deadline_at):
                    return self._watchdog_overdue_reason(watchdog)
        spawn = self._spawns[worker_id]
        if self._startup_clock_expired(spawn, now):
            return 'worker-startup-timeout'
        lifecycle = spawn.lifecycle
        if lifecycle is not None or not spawn.health_inventory_complete:
            return None
        arrival = spawn.worker.last_arrival_at
        if arrival is not None:
            spawn.health_deadline = (
                arrival + self._config.daemon.health_interval_secs * _WORKER_STALE_INTERVALS
            )
        if now >= spawn.health_deadline:
            return 'worker-heartbeat-overdue'
        return None

    def _accept_worker_health(
        self,
        worker: _WorkerHandle,
        message: HealthUpdate,
    ) -> bool:
        """Accept health from its current author and contain any new failure."""
        now = self._clock()
        try:
            accepted = self._record_worker_health(worker, message, now)
        except (TypeError, ValueError):
            self._contain_current_worker(
                worker,
                'watchdog-sampling-failed',
                watchdog_failure=True,
            )
            return True
        if not accepted:
            return False
        reason = (
            self._spawns[worker.worker_id].health_failure
            if self._message_batches is not None
            else self._worker_health_failure(
                worker.worker_id,
                self._spawns[worker.worker_id].health,
                now,
            )
        )
        if reason is None:
            return False
        return self._contain_current_worker(
            worker,
            reason,
            watchdog_failure=reason == 'watchdog-sampling-failed',
        )

    def _accept_worker_channel_message(
        self,
        worker: _WorkerHandle,
        message: ControlMessage,
    ) -> bool | None:
        """Accept common channel traffic independently of its current receive owner."""
        # None is unhandled; False is handled/live; True is handled/terminal.
        if isinstance(message, HealthUpdate):
            return self._accept_worker_health(worker, message)
        if (
            isinstance(message, EventPeerReady)
            and message in self._stale_grant_readies
            and (spawn := self._spawns.get(worker.worker_id)) is not None
            and spawn.worker is worker
            and self._source_workers.get((worker.spec.site, message.source)) is worker
        ):
            self._stale_grant_readies.remove(message)
            return False
        if isinstance(message, GroupRecoveryReady):
            self._accept_group_recovery_ready(worker, message)
            return False
        return None

    def _accept_dependent_grant_message(
        self,
        lifecycle: _WorkerLifecycle,
        grant: EventPeerGrant,
        source_worker: _WorkerHandle,
        message: ControlMessage,
    ) -> bool:
        """Route one source-channel message while a dependent owns its grant."""
        if self._accept_worker_channel_message(source_worker, message) is not None:
            return False
        if isinstance(message, CommandResult):
            return False
        expected = EventPeerReady(
            source=grant.source,
            group=grant.group,
            dependent_spawn_instance=grant.dependent_spawn_instance,
        )
        if message != expected:
            raise ValueError('worker event grant acknowledgement conflicts')
        lifecycle.grant_index += 1
        lifecycle.grant_sent = False
        return lifecycle.grant_index >= len(lifecycle.assignment.event_grants)

    def _accept_operator_batch(
        self,
        worker: _WorkerHandle,
        command: OperatorCommand,
        messages: Iterator[ControlMessage],
    ) -> CommandResult | None:
        """Retain the correlated result until its finite health batch is judged."""
        if self._message_batches is not None:
            raise RuntimeError('worker message acquisition is not reentrant')
        result = None
        self._message_batches = {}
        try:
            for message in self._receive_operator_messages(worker, messages):
                retired = self._accept_worker_channel_message(worker, message)
                if retired is True:
                    break
                if retired is None:
                    if isinstance(message, CommandResult):
                        if message.request_id == command.request_id:
                            if message.command != command.command:
                                raise ValueError('operator command result identity conflicts')
                            result = message
                    else:
                        raise TypeError(
                            f'unexpected operator response from worker {worker.worker_id}: '
                            f'{type(message).__name__}'
                        )
                self._advance_progress()
        except _WorkerControlError as failure:
            self._contain_worker_control_failure(failure)
        finally:
            self._message_batches = None
        now = self._clock()
        reason = self._worker_health_failure(
            worker.worker_id, self._spawns[worker.worker_id].health, now
        )
        if reason is not None:
            self._contain_current_worker(worker, reason)
        return result

    def _accept_worker_monitor_message(
        self,
        worker: _WorkerHandle,
        message: ControlMessage,
    ) -> bool:
        """Accept one ordinary monitor message and report channel retirement."""
        retired = self._accept_worker_channel_message(worker, message)
        if retired is not None:
            return retired
        if isinstance(message, CommandResult):
            return False
        raise TypeError(
            f'unexpected monitor message from worker {worker.worker_id}: {type(message).__name__}'
        )

    def _advance_dependent_grant(
        self,
        lifecycle: _WorkerLifecycle,
    ) -> bool:
        """Advance one dependent grant and report whether all grants are ready."""
        grants = lifecycle.assignment.event_grants
        if lifecycle.grant_index >= len(grants):
            return True
        grant = grants[lifecycle.grant_index]
        worker = lifecycle.worker
        source_worker = self._source_workers[(worker.spec.site, grant.source)]
        if (
            source_worker is self._dispatch_reserved
            or self._spawns[source_worker.worker_id].termination_reason is not None
        ):
            return False
        owner = self._active_dependent_grant_owner(source_worker)
        if owner is not None and owner is not lifecycle:
            return False
        if not lifecycle.grant_sent:
            with self._normalize_worker_control_errors(source_worker, sending=True):
                source_worker.send(grant)
            lifecycle.grant_sent = True
        try:
            with self._normalize_worker_control_errors(source_worker):
                message = self._receive_worker_message(source_worker)
        except TimeoutError:
            return False
        return self._accept_dependent_grant_message(
            lifecycle,
            grant,
            source_worker,
            message,
        )

    def _advance_ready_lifecycle(
        self,
        lifecycle: _WorkerLifecycle,
    ) -> bool:
        """Ingest health or recovery while a ready spawn remains fenced."""
        worker = lifecycle.worker
        try:
            with self._normalize_worker_control_errors(worker):
                message = self._receive_worker_message(worker)
        except TimeoutError:
            return False
        if self._accept_worker_channel_message(worker, message) is None:
            raise TypeError('worker lifecycle message conflicts')
        return True

    def _advance_dependent_lifecycle(
        self,
        lifecycle: _WorkerLifecycle,
    ) -> bool:
        """Advance one dependent through auth, grants, and assignment."""
        if lifecycle.ready:
            return self._advance_ready_lifecycle(lifecycle)
        worker = lifecycle.worker
        if not lifecycle.authenticated:
            with self._normalize_worker_control_errors(worker):
                authenticated = worker.authenticate(timeout=0.0)
            if not authenticated:
                return False
            lifecycle.authenticated = True
        if not self._advance_dependent_grant(lifecycle):
            return True
        with self._normalize_worker_control_errors(worker, sending=True):
            worker.assign(lifecycle.assignment)
        self._restart_startup_clock(lifecycle)
        lifecycle.assigned = True
        lifecycle.ready = True
        self._register_worker(worker)
        self._complete_worker_lifecycle(lifecycle)
        return True

    def _advance_source_message(
        self,
        lifecycle: _WorkerLifecycle,
        message: ControlMessage,
    ) -> bool:
        """Ingest one source startup message and report endpoint readiness."""
        worker = lifecycle.worker
        if self._accept_worker_channel_message(worker, message) is not None:
            return False
        if not isinstance(message, SourceEndpointReady):
            raise TypeError('worker source endpoint message conflicts')
        expected_generations = dict(lifecycle.assignment.source_generations)
        if (
            message.source in lifecycle.endpoint_messages
            or expected_generations.get(message.source) != message.source_generation
        ):
            raise ValueError('worker source endpoint identity conflicts')
        lifecycle.endpoint_messages[message.source] = message
        return set(lifecycle.endpoint_messages) == set(expected_generations)

    def _advance_source_lifecycle(
        self,
        lifecycle: _WorkerLifecycle,
    ) -> bool:
        """Advance one source owner through auth, assignment, and endpoints."""
        if lifecycle.ready:
            return self._advance_ready_lifecycle(lifecycle)
        worker = lifecycle.worker
        if not lifecycle.authenticated:
            with self._normalize_worker_control_errors(worker):
                authenticated = worker.authenticate(timeout=0.0)
            if not authenticated:
                return False
            lifecycle.authenticated = True
        if not lifecycle.assigned:
            with self._normalize_worker_control_errors(worker, sending=True):
                worker.assign(lifecycle.assignment)
            self._restart_startup_clock(lifecycle)
            lifecycle.assigned = True
        try:
            with self._normalize_worker_control_errors(worker):
                message = self._receive_worker_message(worker)
        except TimeoutError:
            return True
        if not self._advance_source_message(lifecycle, message):
            return True
        lifecycle.ready = True
        self._register_worker(worker)
        for source, endpoint in lifecycle.endpoint_messages.items():
            key = (worker.spec.site, source)
            self._source_endpoints[key] = endpoint
            self._source_workers[key] = worker
        self._start_ready_dependents(worker.spec.site)
        self._complete_worker_lifecycle(lifecycle)
        return True

    def _advance_lifecycles(self) -> bool:
        """Give every worker lifecycle one nonblocking state-machine step."""
        changed = False
        grant_workers = self._grant_worker_ids()
        for worker_id, lifecycle in tuple(
            (worker_id, spawn.lifecycle)
            for worker_id, spawn in self._spawns.items()
            if spawn.lifecycle is not None
        ):
            if (
                self._spawns[worker_id].termination_reason is not None
                or self._spawns[worker_id].lifecycle is not lifecycle
                or lifecycle.worker is self._dispatch_reserved
            ):
                continue
            if worker_id in grant_workers:
                continue
            try:
                if lifecycle.assignment.source_generations:
                    progressed = self._advance_source_lifecycle(lifecycle)
                else:
                    progressed = self._advance_dependent_lifecycle(lifecycle)
            except _WorkerControlError as failure:
                self._contain_worker_control_failure(failure)
                progressed = True
            except (EOFError, TypeError, ValueError) as error:
                progressed = self._contain_current_worker(
                    lifecycle.worker, f'worker-startup-{type(error).__name__}'
                )
            changed = progressed or changed
            self._advance_progress()
        return changed

    def _ingest_worker_ipc(self) -> bool:
        """Drain spawn-matched health, stale-grant, and late `CommandResult` traffic."""
        ingested = False
        grant_workers = self._grant_worker_ids()
        for worker_id, spawn in tuple(self._spawns.items()):
            worker = spawn.worker
            if (
                self._spawns[worker_id].lifecycle is not None
                or self._spawns[worker_id].termination_reason is not None
                or worker_id in grant_workers
                or worker is self._dispatch_reserved
            ):
                continue
            while True:
                try:
                    message = self._receive_worker_message(worker)
                except TimeoutError:
                    break
                except (EOFError, OSError, ValueError):
                    self._fail_current_worker(worker, 'worker-control-eof')
                    ingested = True
                    self._advance_progress()
                    break
                try:
                    retired = self._accept_worker_monitor_message(worker, message)
                except (TypeError, ValueError) as error:
                    if not isinstance(message, GroupRecoveryReady):
                        raise
                    retired = self._contain_current_worker(
                        worker, f'worker-startup-{type(error).__name__}'
                    )
                ingested = True
                self._advance_progress()
                if retired:
                    break
            self._advance_progress()
        return ingested

    def _monitor_worker_health(self, now: float) -> bool:
        """Terminate every worker whose current spawn misses its health deadline."""
        changed = False
        for worker_id, spawn in tuple(self._spawns.items()):
            worker = spawn.worker
            if self._spawns[worker_id].termination_reason is not None:
                continue
            health = self._spawns[worker_id].health
            if health is not None:
                health = self._refresh_component_health(health, now)
                self._spawns[worker_id].health = health
            reason = self._worker_health_failure(worker_id, health, now)
            if reason is None:
                pending_recovery = bool(spawn.pending_recovery)
                if (
                    health is not None
                    and self._spawns[worker_id].health_inventory_complete
                    and {group.group for group in health.groups}
                    == set(worker.spec.groups) | self._pending_worker_groups(worker)
                    and not pending_recovery
                    and self._breakers[worker_id].record_healthy()
                ):
                    state = self._state
                    if state is None:
                        raise RuntimeError('supervisor state is not open')
                    state.persist_worker_breaker(self._breakers[worker_id])
                    self._advance_progress()
                self._advance_progress()
                continue
            changed = self._contain_current_worker(worker, reason) or changed
            self._advance_progress()
        return changed

    def _advance_worker_messages(self) -> bool:
        """Route one closed batch per spawn before judging temporal liveness."""
        if self._message_batches is not None:
            raise RuntimeError('worker message acquisition is not reentrant')
        self._message_batches = {}
        self._batch_message_count = 0
        changed = False
        try:
            while True:
                consumed = self._batch_message_count
                grant_workers = self._grant_worker_ids()
                changed = self._advance_lifecycles() or changed
                if (
                    self._batch_message_count == consumed
                    and self._grant_worker_ids() == grant_workers
                ):
                    break
            changed = self._ingest_worker_ipc() or changed
        finally:
            self._message_batches = None
        return self._monitor_worker_health(self._clock()) or changed

    def _replace_worker(self, worker_id: str, *, source_owner_adoption: bool = False) -> None:
        """Replace one worker with a fresh spawn; caller owns coordination."""
        spec = self._specs[worker_id]
        if spec.sources and not source_owner_adoption:
            raise ValueError('source-owning worker restart requires dependent coordination')
        old = self._spawns[worker_id].worker
        self._shutdown_worker(old, 'worker-restart')
        self._discard_worker_lifecycle(old)
        if spec.sources:
            self._release_source_ownership(old)
            self._spawn_source(spec)
        else:
            self._spawn_dependent(spec.site, spec)
        self._advance_worker_messages()

    def _worker_monitor_deadline(self, spawn: _WorkerSpawn, now: float) -> float | None:
        """Wake on future arrival silence instead of a passed startup threshold."""
        lifecycle = spawn.lifecycle
        if lifecycle is not None and self._lifecycle_unclocked(lifecycle):
            return None
        clock = self._startup_clock(spawn)
        if clock is None:
            return spawn.health_deadline
        _started_at, deadline = clock
        if now < deadline:
            return deadline
        arrival = spawn.worker.last_arrival_at
        silence = (
            now
            if arrival is None
            else (arrival + _WORKER_STALE_INTERVALS * self._config.daemon.health_interval_secs)
        )
        return (
            silence if silence > now else now + self._config.daemon.supervisor_poll_interval_secs
        )

    def _next_monitor_deadline(self) -> float:
        """Return the earliest active monitor deadline."""
        next_checkpoint_at = self._next_checkpoint_at
        if next_checkpoint_at is None:
            raise RuntimeError('supervisor is not open')
        deadlines = [next_checkpoint_at]
        now = self._clock()
        for spawn in self._spawns.values():
            if spawn.termination_reason is None:
                deadline = self._worker_monitor_deadline(spawn, now)
                if deadline is not None:
                    deadlines.append(deadline)
        for worker_id, deadline in self._restart_deadlines.items():
            if worker_id in self._spawns:
                continue
            spec = self._specs[worker_id]
            if spec.sources or self._dependent_sources_ready(spec):
                deadlines.append(deadline)
        return min(deadlines)

    def _wait_for_replacement_readiness(self, replacements: dict[str, _WorkerHandle]) -> None:
        """Wait one positive bounded pass only while replacements need readiness."""
        if not replacements:
            return
        now = self._clock()
        deadline_at = min(
            (
                deadline
                for worker_id in replacements
                if (deadline := self._worker_monitor_deadline(self._spawns[worker_id], now))
                is not None
            ),
            default=now + self._config.daemon.supervisor_poll_interval_secs,
        )
        remaining = max(
            self._config.daemon.supervisor_poll_interval_secs if deadline_at <= now else 0,
            deadline_at - now,
        )
        self._advance_progress()
        time.sleep(
            min(
                remaining,
                self._config.daemon.supervisor_poll_interval_secs,
            )
        )

    def _restart_source_owners(
        self,
        site: str,
        source: str,
        groups: Collection[str],
        result: CommandResult,
    ) -> CommandResult:
        """Rebuild source inventory and dependent lanes after durable recovery."""

        def failed_restart(
            worker_id: str, reason: str, *, stopping: bool = False
        ) -> CommandResult:
            return replace(
                result,
                ok=False,
                message=(
                    f'{result.command} committed; worker {worker_id} then failed '
                    f'to restart ({reason}). '
                    + (
                        'The supervisor is stopping; restart it before running status. '
                        'Do not repeat the recovery.'
                        if stopping
                        else 'Run status to see its state.'
                    )
                ),
            )

        source_worker_ids, dependent_ids = self._source_adoption_restart_ids(
            site,
            source,
            groups,
        )
        for worker_id in dependent_ids:
            worker = self._spawns[worker_id].worker
            self._shutdown_worker(worker, 'worker-restart')
            self._discard_worker_lifecycle(worker)
            self._advance_progress()
        spawn_phases: tuple[tuple[tuple[str, ...], Callable[[str], object]], ...] = (
            (
                source_worker_ids,
                lambda worker_id: self._replace_worker(worker_id, source_owner_adoption=True),
            ),
            (
                dependent_ids,
                lambda worker_id: self._spawn_dependent(
                    self._specs[worker_id].site,
                    self._specs[worker_id],
                ),
            ),
        )
        for worker_ids, spawn in spawn_phases:
            replacements: dict[str, _WorkerHandle] = {}
            for worker_id in worker_ids:
                try:
                    spawn(worker_id)
                except (OSError, RuntimeError, sqlite3.Error, TypeError, ValueError) as error:
                    self._maintenance_failure = error
                    return failed_restart(worker_id, str(error), stopping=True)
                replacements[worker_id] = self._spawns[worker_id].worker
                self._advance_progress()
            while replacements:
                self._advance_worker_messages()
                self._progress.publish(self._probe_snapshot(self._clock()))
                self._advance_progress()
                for worker_id, worker in tuple(replacements.items()):
                    reason = self._spawns[worker_id].termination_reason
                    if reason is not None:
                        return failed_restart(worker_id, reason)
                    lifecycle = self._spawns[worker_id].lifecycle
                    ready = (lifecycle is None or lifecycle.ready) and (
                        not worker.spec.sources
                        or all(
                            self._source_workers.get((worker.spec.site, source_name)) is worker
                            for source_name in worker.spec.sources
                        )
                    )
                    if ready:
                        replacements.pop(worker_id)
                    self._advance_progress()
                self._wait_for_replacement_readiness(replacements)
        return result

    def _resume_completed_disposition(
        self,
        site: str,
        active: ActiveSourceGroupAssignment,
        result: CommandResult,
    ) -> CommandResult:
        """Resume a pending group only after its completion is durable."""
        group = active.group
        pending = self._reconciled is not None and isinstance(
            self._reconciled.get((site, group)), PendingDispositionSourceGroupAssignment
        )
        if self._reconciled is not None:
            self._reconciled[(site, group)] = active
        if pending:
            return self._restart_source_owners(site, active.source, (group,), result)
        return result

    def _complete_disposition_inventory(
        self,
        site: str,
        reservation: PendingDispositionSourceGroupAssignment,
        result: CommandResult,
    ) -> CommandResult:
        """Complete only the matching reservation, preserving any newer pending intent."""
        state = self._state
        if state is None:
            raise RuntimeError('group disposition authority is unavailable')
        group = reservation.group
        key = (site, group, reservation.reserved_generation)
        if (
            'completion',
            site,
            group,
            reservation.reserved_generation,
        ) in state.pending.writes:
            return result
        record = state.group_inventory_record(site=site, group=group)
        if (
            record is None
            or record.generation != reservation.reserved_generation
            or record.orphan_generation != reservation.orphan_generation
        ):
            return result
        if not record.configured:
            state.complete_orphan_disposition(
                site=site,
                group=group,
                orphan_generation=reservation.orphan_generation,
                reserved_generation=reservation.reserved_generation,
            )
            return result
        active = state.complete_group_disposition(
            site=site,
            group=group,
            orphan_generation=reservation.orphan_generation,
            reserved_generation=reservation.reserved_generation,
        )
        if (
            'completion',
            site,
            group,
            reservation.reserved_generation,
        ) in state.pending.writes:
            self._pending_disposition_resumes[key] = (active, result)
            return result
        return self._resume_completed_disposition(site, active, result)

    def _resume_stored_dispositions(self) -> None:
        """Drain committed continuations once, including commits made by operators."""
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        for key, (active, result) in tuple(self._pending_disposition_resumes.items()):
            site, group, generation = key
            if ('completion', site, group, generation) in state.pending.writes:
                continue
            del self._pending_disposition_resumes[key]
            record = state.group_inventory_record(site=site, group=group)
            if (
                record is None
                or record.generation != generation
                or record.lifecycle != 'active'
                or record.orphan_generation is not None
            ):
                continue
            self._resume_completed_disposition(site, active, result)

    def _tick(self) -> bool:
        """Advance maintenance and report lifecycle, monitor, or due checkpoint progress."""
        if self._halt_reason_value is not None:
            raise RuntimeError(f'supervisor is halted: {self._halt_reason_value}')
        state = self._state
        next_checkpoint_at = self._next_checkpoint_at
        if state is None or next_checkpoint_at is None:
            raise RuntimeError('supervisor is not open')
        now = self._clock()
        if not isfinite(now) or now < 0:
            raise ValueError('supervisor clock must be finite and nonnegative')
        had_pending = bool(state.pending.writes)
        flushed = state.flush_pending()
        self._resume_stored_dispositions()
        changed = self._advance_worker_messages() or (had_pending and flushed)
        self._advance_progress()
        changed = self._reap_workers(now) or changed
        self._advance_progress()
        changed = self._restart_due_workers(now) or changed
        self._advance_progress()
        if now >= next_checkpoint_at:
            interval = self._config.daemon.checkpoint_interval_secs
            elapsed_intervals = int((now - next_checkpoint_at) // interval) + 1
            self._next_checkpoint_at = next_checkpoint_at + elapsed_intervals * interval
            try:
                state.checkpoint()
            except DatabaseError as error:
                self._halt_reason_value = str(error)
                self._progress.set_halted()
                self._shutdown_after_failure('supervisor-checkpoint-failure', error)
                raise
            self._advance_progress()
            changed = True
        generated_at = self._clock()
        self._progress.publish(self._probe_snapshot(generated_at))
        return changed

    def _advance_operator_wait(self, command: OperatorCommand) -> None:
        """Maintain supervision while preserving failures for the outer run loop."""
        try:
            stopping = self._should_stop is not None and self._should_stop()
        except BaseException as error:
            self._maintenance_failure = error
            raise OperatorOutcomeUnknownError(
                _control.unknown_outcome_message(command.command, 'the supervisor is stopping')
            ) from error
        if stopping:
            raise OperatorOutcomeUnknownError(
                _control.unknown_outcome_message(command.command, 'the supervisor is stopping')
            )
        try:
            self._tick()
        except BaseException as error:
            self._maintenance_failure = error
            raise OperatorOutcomeUnknownError(
                _control.unknown_outcome_message(
                    command.command, f'the supervisor failed: {error}'
                )
            ) from error

    def dispatch_operator_command(
        self,
        command: OperatorCommand,
        *,
        timeout: float,
    ) -> CommandResult:
        """Send one request to its owning worker and correlate the result.

        Health and channel failures contain the owning worker. A response wait
        cap leaves it running. Neither failure rolls back a dispatched mutation
        or establishes its outcome; ordinary supervision continues while
        waiting.

        Args:
            command: Trusted body-free operator command.
            timeout: Positive maximum response wait in seconds after dispatch.

        Returns:
            Exact request-correlated worker result.

        Raises:
            OperatorOutcomeUnknownError: The dispatched command has no definite
                outcome.
            ValueError: If the timeout is not positive, the target is invalid,
                or the response conflicts.
            TypeError: The target or worker response has the wrong type.
            sqlite3.Error: Reading durable inventory fails.
            DatabaseError: Persisting worker-containment state fails.
            RuntimeError: Durable inventory or containment authority is
                inconsistent.
            OSError: Signaling a failed worker during containment fails.
        """
        if timeout <= 0:
            raise ValueError('operator command timeout must be positive')
        worker = self._operator_command_owner(command)
        transport_command = self._next_operator_transport_command(command)
        self._send_operator_command(worker, transport_command)
        self._dispatch_reserved = worker
        try:
            deadline = time.monotonic() + timeout
            self._advance_progress()
            while True:
                try:
                    message = self._receive_operator_response(worker, deadline)
                    if message is not None:
                        result = self._accept_operator_batch(
                            worker,
                            transport_command,
                            self._receive_operator_batch(worker, message),
                        )
                        if result is not None:
                            if result.result.get(OUTCOME_KEY) == OUTCOME_UNKNOWN:
                                raise OperatorOutcomeUnknownError(
                                    _control.unknown_outcome_message(
                                        command.command, 'the worker stopped while running it'
                                    )
                                )
                            return replace(result, request_id=command.request_id)
                        if self._spawns[worker.worker_id].termination_reason is not None:
                            raise OperatorOutcomeUnknownError(
                                _control.unknown_outcome_message(
                                    command.command, 'the worker failed while running it'
                                )
                            )
                except _WorkerControlError as failure:
                    self._contain_worker_control_failure(failure)
                    raise OperatorOutcomeUnknownError(
                        _control.unknown_outcome_message(
                            command.command, 'the worker failed while running it'
                        )
                    ) from failure
                self._advance_operator_wait(command)
                spawn = self._spawns.get(worker.worker_id)
                if (
                    spawn is None
                    or spawn.worker is not worker
                    or spawn.termination_reason is not None
                ):
                    raise OperatorOutcomeUnknownError(
                        _control.unknown_outcome_message(
                            command.command, 'the worker failed while running it'
                        )
                    )
                if time.monotonic() >= deadline:
                    raise OperatorOutcomeUnknownError(
                        _control.unknown_outcome_message(
                            command.command,
                            'the worker did not answer within '
                            + (
                                f'{timeout / _SECONDS_PER_MINUTE:g} minutes'
                                if timeout >= _SECONDS_PER_MINUTE
                                and timeout % _SECONDS_PER_MINUTE == 0
                                else f'{timeout:g} second{"" if timeout == 1 else "s"}'
                            ),
                        )
                    )
        finally:
            self._dispatch_reserved = None

    def _dispatch_source_adoption(
        self,
        command: OperatorCommand,
        *,
        target: tuple[str, str, str],
        reservation: tuple[str, str],
        groups: Collection[str],
        timeout: float,
    ) -> CommandResult:
        """Commit source adoption, validate initialization, and restart lanes."""
        adoption_id, command_fingerprint = reservation
        result = self.dispatch_operator_command(
            OperatorCommand(
                request_id=command.request_id,
                command=command.command,
                arguments={
                    **command.arguments,
                    'adoption_id': adoption_id,
                    'command_fingerprint': command_fingerprint,
                    'groups_parked': True,
                },
                identity=command.identity,
            ),
            timeout=timeout,
        )
        if not result.ok:
            names = ', '.join(groups) or 'none'
            return replace(
                result,
                message=f'{result.message}; already parked and reserved: {names}',
            )
        if (
            result.result.get('initialized') is not True
            or result.result.get('status') != 'running'
        ):
            raise RuntimeError('source adoption returned before replacement initialization')
        site, source, _reason = target
        return self._restart_source_owners(site, source, groups, result)

    def _fence_source_adoption_groups(
        self,
        command: OperatorCommand,
        *,
        target: tuple[str, str, str],
        timeout: float,
    ) -> tuple[str, ...]:
        """Park and durably reserve every dependent group's disposition."""
        site, source, reason = target
        state = self._state
        if state is None:
            raise RuntimeError('group disposition authority is unavailable')
        groups = tuple(
            sorted(
                group.name
                for group in self._config.sites[site].groups.values()
                if group.source == source
            )
        )
        reserved: list[str] = []
        for group in groups:
            assignment = state.configured_group_assignments().get((site, group))
            if isinstance(assignment, ActiveSourceGroupAssignment):
                park = self.dispatch_operator_command(
                    OperatorCommand(
                        request_id=f'{command.request_id}:park:{group}',
                        command='park',
                        arguments={
                            'site': site,
                            'group': group,
                            'reason': f'source adoption: {reason}',
                        },
                        identity=command.identity,
                    ),
                    timeout=timeout,
                )
                if not park.ok or park.result.get('parked') is not True:
                    names = ', '.join(reserved) or 'none'
                    raise ValueError(
                        f'source adoption could not park group {group!r}: {park.message}; '
                        f'already parked and reserved: {names}'
                    )
                observed_generation = park.result.get('group_generation')
                if type(observed_generation) is not int:
                    raise ValueError('parked group generation is unavailable')
                assignment = state.reserve_group_disposition(
                    site=site,
                    group=group,
                    observed_generation=observed_generation,
                )
                if self._reconciled is not None:
                    self._reconciled[(site, group)] = assignment
            if not isinstance(
                assignment,
                PendingDispositionSourceGroupAssignment,
            ):
                raise TypeError(f'source adoption group {group!r} lacks a disposition fence')
            reserved.append(group)
        return groups

    def _preflight_source_adoption(
        self,
        command: OperatorCommand,
        *,
        timeout: float,
    ) -> tuple[str, str] | CommandResult:
        """Reserve adoption before quiescence or return an exact final result."""
        preflight = self.dispatch_operator_command(
            OperatorCommand(
                request_id=f'{command.request_id}:preflight',
                command='preflight-source-adoption',
                arguments=command.arguments,
                identity=command.identity,
            ),
            timeout=timeout,
        )
        if not preflight.ok:
            return CommandResult(
                command=command.command,
                ok=False,
                message=preflight.message,
                request_id=command.request_id,
                result=preflight.result,
            )
        adoption_id = preflight.result.get('adoption_id')
        command_fingerprint = preflight.result.get('command_fingerprint')
        if (
            not isinstance(adoption_id, str)
            or not adoption_id
            or not isinstance(command_fingerprint, str)
            or not command_fingerprint
        ):
            raise ValueError('source adoption preflight identity is invalid')
        if preflight.result.get('recorded') is not True:
            return adoption_id, command_fingerprint
        return CommandResult(
            command=command.command,
            ok=True,
            message='recorded',
            request_id=command.request_id,
            result={
                key: value
                for key, value in preflight.result.items()
                if key
                not in {
                    'adoption_id',
                    'command_fingerprint',
                    'recorded',
                }
            },
        )

    def _probe_running_disposition(
        self,
        command: OperatorCommand,
        record: _InventoryRecord,
        disposition_id: str,
        timeout: float,
    ) -> tuple[CommandResult | None, bool]:
        """Validate a running lane or return an exact receipt before reserving new work."""
        if not record.configured or record.lifecycle != 'active':
            return None, False
        try:
            self._operator_command_owner(command)
        except ValueError:
            return None, False
        probe = self.dispatch_operator_command(
            replace(
                command,
                arguments={
                    **command.arguments,
                    'disposition_id': disposition_id,
                    'orphan_generation': record.generation,
                    'reserved_generation': record.generation,
                },
            ),
            timeout=timeout,
        )
        return (
            probe
            if not probe.ok or probe.result.get('disposition_required') is not True
            else None,
            True,
        )

    def _route_disposition(
        self,
        command: OperatorCommand,
        metadata: _OrphanGroupMetadata,
        request: DispositionRequest,
        timeout: float,
    ) -> CommandResult:
        """Choose the running lane, ready source owner, or supervisor executor."""
        try:
            self._operator_command_owner(command)
        except ValueError:
            pass
        else:
            return self.dispatch_operator_command(
                replace(
                    command,
                    arguments={
                        **command.arguments,
                        'disposition_id': request.disposition_id,
                        'orphan_generation': metadata.group_generation,
                        'reserved_generation': request.reserved_generation,
                    },
                ),
                timeout=timeout,
            )
        source_command = replace(
            command,
            command='group-disposition',
            arguments={
                **command.arguments,
                'source': metadata.source,
                'command': request.command,
                'disposition_id': request.disposition_id,
                'reserved_generation': request.reserved_generation,
                'expected_revision': request.expected_revision,
                'operator_label': request.operator_label,
                'from_event_id': request.from_event_id,
                'acknowledge_truncation': request.acknowledge_truncation,
                'reason': request.reason,
            },
        )
        try:
            self._operator_command_owner(source_command)
        except ValueError:
            return self._run_direct_disposition(command, metadata, request)
        result = self.dispatch_operator_command(source_command, timeout=timeout)
        return replace(result, command=command.command)

    def _dispatch_group_disposition(
        self, command: OperatorCommand, timeout: float
    ) -> CommandResult:
        """Reserve before source execution and complete after the atomic source commit."""
        site, group = self._group_disposition_target(command)
        disposition = disposition_command(
            command.command,
            command.arguments,
            disposition_id=command.request_id,
            wall_clock_time=self._wall_clock(),
            operator_label=control_peer_label(command.identity),
        )
        # Validate before hashing: malformed fields may contain frozen mappings.
        disposition_id = stable_operator_id(command.command, command.arguments)
        disposition = replace(disposition, disposition_id=disposition_id)
        state = self._state
        if state is None:
            raise RuntimeError('group disposition authority is unavailable')
        record = state.group_inventory_record(site=site, group=group)
        if record is None:
            raise ValueError('group disposition target is not configured')
        probe, lane_admitted = self._probe_running_disposition(
            command, record, disposition_id, timeout
        )
        if probe is not None:
            return probe
        metadata = self._orphan_group_metadata(site=site, group=group)
        if (
            metadata.site != site
            or metadata.source != record.source
            or metadata.group_generation not in {record.generation, record.orphan_generation}
        ):
            raise RuntimeError('group disposition inventory conflicts with group state')
        recorded_generations, revision, admissible = self._disposition_observation(
            site, group, disposition_id
        )
        if recorded_generations is None:
            if record.configured and not lane_admitted and not admissible:
                raise InvalidTransitionError('position changes require a parked or orphaned group')
            reservation = state.reserve_group_disposition(
                site=site, group=group, observed_generation=metadata.group_generation
            )
        else:
            reservation = PendingDispositionSourceGroupAssignment(
                source=metadata.source,
                group=group,
                orphan_generation=recorded_generations[0],
                reserved_generation=recorded_generations[1],
            )
        request = disposition.request(group, reservation.reserved_generation, revision)
        result = self._route_disposition(command, metadata, request, timeout)
        if not result.ok:
            return replace(
                result,
                message=f'{result.message}; its durable disposition reservation was kept',
            )
        if result.result.get('group_generation') != reservation.reserved_generation:
            raise RuntimeError('group disposition result generation conflicts')
        if recorded_generations is not None:
            result = replace(result, message='recorded')
        return self._complete_disposition_inventory(site, reservation, result)

    def adopt_source_stream_command(
        self,
        command: OperatorCommand,
        *,
        timeout: float,
    ) -> CommandResult:
        """Park every group fed by the source before dispatching its adoption.

        A recorded preflight result returns without new work. Fresh work parks
        and fences groups after durable adoption reservation. Adoption commits
        before worker restarts, so failure can leave prior groups parked or
        reserved, or leave the replacement source committed.

        Args:
            command: Authenticated source adoption request.
            timeout: Positive per-owner response deadline in seconds.

        Returns:
            Recorded preflight result or correlated source-owner adoption
            result.

        Raises:
            ValueError: The command, target, or timeout is invalid, adoption is
                already active, or a group cannot be parked.
            OperatorOutcomeUnknownError: A dispatched command has no definite
                outcome.
            RuntimeError: Required disposition authority is unavailable or the
                adoption result does not confirm initialized running state.
            TypeError: The target or worker response has the wrong type.
            sqlite3.Error: Reading durable inventory or retained disposition
                evidence fails.
            DatabaseError: Persisting adoption fences or worker state fails.
            OSError: Preparing state paths or stopping or restarting affected
                workers fails.
            Exception: An injected worker lifecycle operation fails.
        """
        try:
            target = self._source_adoption_target(command)
            site_name, source_name, _ = target
            identity = (site_name, source_name)
            if identity in self._adopting_sources:
                raise ValueError('source adoption is already in progress')
            preflight = self._preflight_source_adoption(
                command,
                timeout=timeout,
            )
            if isinstance(preflight, CommandResult):
                return preflight
            adoption_id, command_fingerprint = preflight
            self._adopting_sources.add(identity)
            try:
                groups = self._fence_source_adoption_groups(
                    command,
                    target=target,
                    timeout=timeout,
                )
                return self._dispatch_source_adoption(
                    command,
                    target=target,
                    reservation=(adoption_id, command_fingerprint),
                    groups=groups,
                    timeout=timeout,
                )
            finally:
                self._adopting_sources.discard(identity)
        except (SourceDatabaseBusyError, _SupervisorDatabaseBusyError) as error:
            return _busy_operator_result(command, error)

    def dispatch_group_disposition_command(
        self,
        command: OperatorCommand,
        *,
        timeout: float,
    ) -> CommandResult:
        """Reserve, route, and promote one replay or reset disposition.

        Durable generation reservation precedes owner execution and successful
        promotion. A failed result, response timeout, or later conflict can
        leave pending durable disposition work; failure does not imply
        rollback.

        Args:
            command: Trusted replay or reset operator command.
            timeout: Per-owner response deadline in seconds.

        Returns:
            Exact request-correlated group disposition result.

        Raises:
            TypeError: The target or worker response has the wrong type.
            ValueError: The command, target, or response is invalid.
            OperatorOutcomeUnknownError: A dispatched command has no definite
                outcome.
            InvalidTransitionError: The group is active and unparked, or direct
                disposition execution rejects its state, authority, or
                requested position.
            RuntimeError: Disposition authority or durable state conflicts.
            sqlite3.Error: Reading source, group, or supervisor disposition
                evidence fails.
            DatabaseError: Opening state or committing disposition or inventory
                changes fails.
            SecurityError: A direct-execution state or lock path fails safety
                checks.
            OSError: Resolving state paths, inspecting the WAL, or acquiring or
                releasing ownership locks fails.
            RevisionConflictError: Direct group execution loses its expected
                revision.
        """
        try:
            return self._dispatch_group_disposition(command, timeout)
        except (SourceDatabaseBusyError, _SupervisorDatabaseBusyError) as error:
            return _busy_operator_result(command, error)

    def probe_result(self) -> ProbeResult:
        """Return one I/O-free request-time liveness and readiness result.

        Returns:
            Immutable probe result from foreground progress and snapshot state.
        """
        return self._progress.result(now=self._clock())

    def probe_mapping(self) -> Mapping[str, object]:
        """Return the exact compact control projection from one observation.

        Returns:
            Compact liveness and readiness probe fields.
        """
        result = self.probe_result()
        return {
            'liveness': result.liveness,
            'readiness': result.readiness,
            'complete': result.complete,
            'conditions': result.conditions,
            'generated_at': result.generated_at,
            'snapshot_age_seconds': result.snapshot_age_seconds,
        }

    def reconcile_durable_state(
        self,
        should_stop: Callable[[], bool] = lambda: False,
    ) -> None:
        """Discover locked durable authorities and reconcile before spawning.

        This startup boundary opens every existing configured or retained
        source and group database under its owner lock, performs disposition
        recovery, and releases those locks before worker construction.

        Args:
            should_stop: Return true to interrupt a supervisor database
                acquisition wait.

        Raises:
            ConfigError: A packed worker or reconciliation authority set
                exceeds the resulting open-file limit, including its margin.
            RuntimeError: The supervisor is not open, workers have already been
                spawned, or retained source/group authorities cannot be
                reconciled, or `should_stop` interrupted startup.
            AlreadyLockedError: A required source or group authority remains
                owned.
            SecurityError: An authority path or lock fails filesystem safety
                checks.
            sqlite3.Error: Reading retained inventory or source/group metadata
                fails.
            DatabaseError: Opening, validating, or updating an authority fails,
                or write acquisition is stopped.
            OSError: Preparing authority paths, inspecting the WAL, or
                acquiring or releasing locks fails.
            ValueError: A retained generation, manifest, snapshot, or identity
                is invalid.
        """
        if self._spawns:
            raise RuntimeError('startup reconciliation cannot run after worker spawn')
        state = self._state
        if state is None:
            raise RuntimeError('supervisor is not open')
        soft, hard = raise_open_file_limit()
        _LOGGER.info('open-file limit %s (hard limit %s)', soft, hard)
        for site in self._config.sites.values():
            for spec in build_worker_specs(site):
                need = open_file_need(site, spec) + OPEN_FILE_MARGIN
                if soft != resource.RLIM_INFINITY and need > soft:
                    raise ConfigError(
                        f'worker {spec.worker_id} needs {need} open files '
                        f'(including {OPEN_FILE_MARGIN} margin); limit {soft}'
                    )
        configured = {
            (site.name, group.name): group.source
            for site in self._config.sites.values()
            for group in site.groups.values()
        }
        records = state.inventory_records()
        group_identities = set(configured) | set(records)
        source_identities = {
            (site.name, source.name)
            for site in self._config.sites.values()
            for source in site.sources.values()
        } | {(site, record.source) for (site, _group), record in records.items()}
        need = (
            _supervisor_open_file_need(len(source_identities), len(group_identities))
            + OPEN_FILE_MARGIN
        )
        if soft != resource.RLIM_INFINITY and need > soft:
            raise ConfigError(
                f'supervisor needs {need} open files '
                f'(including {OPEN_FILE_MARGIN} margin); limit {soft}'
            )
        with state.starting(should_stop), ExitStack() as stack:
            source_states = self._open_reconciliation_sources(stack, source_identities)
            generations, group_states = self._open_reconciliation_groups(
                stack,
                group_identities,
                configured,
                source_states,
            )
            self._reconcile(
                generations=generations,
                source_states=source_states,
                group_states=group_states,
            )

    def reset_worker_breaker_command(
        self,
        arguments: Mapping[str, object],
        *,
        identity: AuditIdentity,
        wall_clock_time: float,
    ) -> Mapping[str, object]:
        """Apply one authenticated audited worker-breaker reset.

        Args:
            arguments: Exact body-free site, worker, and reason fields.
            identity: Kernel-authenticated operator identity.
            wall_clock_time: Finite nonnegative epoch seconds for the audit.

        Returns:
            Structured closed-breaker transition.

        Raises:
            ValueError: Arguments or audit time are invalid, the target is not
                configured, the reason exceeds 4096 UTF-8 bytes or contains
                secret-looking text, or the breaker is not open at the expected
                revision.
            RuntimeError: The supervisor or breaker authority is unavailable.
            ControlCommandError: Another connection holds supervisor.db; the
                retryable result carries code `supervisor-database-busy`.
            DatabaseError: Applying pending state or committing the audited
                reset fails.
            Exception: A pending transition fails while applying the reset
                transaction.
        """
        if set(arguments) != {'reason', 'site', 'worker'}:
            raise ValueError('worker breaker reset arguments are invalid')
        site = arguments['site']
        worker_id = arguments['worker']
        reason = arguments['reason']
        if not isinstance(site, str) or not site:
            raise ValueError('worker breaker reset site must be nonempty text')
        if not isinstance(worker_id, str) or not worker_id:
            raise ValueError('worker breaker reset worker must be nonempty text')
        if not isinstance(reason, str) or not reason:
            raise ValueError('worker breaker reset fields must be nonempty text')
        if looks_secret(reason):
            raise ValueError('--reason must not contain secret-looking text')
        spec = self._specs.get(worker_id)
        if spec is None or spec.site != site:
            configured = ', '.join(
                sorted(
                    candidate.worker_id
                    for candidate in self._specs.values()
                    if candidate.site == site
                )
            )
            raise ValueError(
                f'worker breaker reset target {worker_id!r} is not configured for {site!r}; '
                f'choose one of: {configured or "none"}'
            )
        breaker = self._breakers.get(worker_id)
        state = self._state
        if breaker is None or state is None:
            raise RuntimeError('worker breaker reset authority is unavailable')
        transition = state.reset_worker_breaker(
            breaker,
            expected_revision=breaker.revision,
            reason=reason,
            identity=identity,
            wall_clock_time=wall_clock_time,
        )
        if worker_id not in self._spawns:
            self._restart_deadlines[worker_id] = self._clock()
        return {
            'worker_id': transition.worker_id,
            'sequence': transition.sequence,
            'state': transition.state,
            'reason': transition.reason,
            'revision': breaker.revision,
        }

    def run(
        self,
        *,
        should_stop: Callable[[], bool],
        wait: Callable[[float], object],
        set_foreground_progress: Callable[[Callable[[], None] | None], None],
    ) -> None:
        """Monitor workers until requested to stop.

        Each maintenance iteration advances staged worker lifecycles, ingests
        IPC, contains health failures, reaps and restarts children, publishes a
        probe, and performs any due checkpoint.
        Progress can therefore come from any of those operations, independently
        of checkpointing.

        Args:
            should_stop: Return `True` after a local control or shutdown
                request.
            wait: Interruptible outer-loop wait for a control message or
                timeout.
            set_foreground_progress: Bind progress to completed foreground wait
                substeps; receive `None` on every exit to release the binding.

        Raises:
            ValueError: The monotonic clock is not finite and nonnegative.
            RuntimeError: The supervisor is unopened or halted.
            DatabaseError: Persisting pending transitions or checkpointing
                fails.
                A checkpoint failure halts the supervisor and attempts worker
                shutdown before propagating.
            BaseException: A supplied stop, wait, or progress callback or a
                worker lifecycle operation fails. Failures saved during an
                operator wait propagate after the command is answered.
            OSError: Checkpoint file inspection or synchronization, or a worker
                lifecycle operation, fails.
            sqlite3.Error: Reading durable inventory during maintenance fails.
        """
        self._should_stop = should_stop
        try:
            set_foreground_progress(self._advance_progress)
            self._advance_progress()
            while not should_stop():
                self._tick()
                if should_stop():
                    return
                now = self._clock()
                if not isfinite(now) or now < 0:
                    raise ValueError('supervisor clock must be finite and nonnegative')
                timeout = min(
                    self._config.daemon.supervisor_poll_interval_secs,
                    max(0.0, self._next_monitor_deadline() - now),
                )
                self._advance_progress()
                try:
                    wait(timeout)
                finally:
                    if self._maintenance_failure is not None:
                        failure = self._maintenance_failure
                        self._maintenance_failure = None
                        raise failure
        finally:
            self._should_stop = None
            set_foreground_progress(None)

    def start(self, should_stop: Callable[[], bool] = lambda: False) -> None:
        """Begin source-first staged startup without waiting for readiness.

        Args:
            should_stop: Return true to interrupt breaker restoration before
                spawning.

        Raises:
            RuntimeError: The supervisor is not open, durable state has not
                been reconciled, workers are already started, or `should_stop`
                interrupted startup, retained breaker state is inconsistent, or
                failed construction leaves an unreaped child.
            OSError: A worker process or its control channel could not be
                created.
            ConfigError: A control or source socket candidate is unusable.
            sqlite3.Error: Reading retained inventory or breaker state fails.
            DatabaseError: Restoring breaker timing or persisting startup state
                fails.
            ValueError: Retained breaker state or a generated assignment is
                invalid.
            Exception: The supplied worker factory fails; startup containment
                is attempted before propagation.
        """
        if self._lock is None or self._state is None:
            raise RuntimeError('supervisor is not open')
        if self._reconciled is None:
            raise RuntimeError('supervisor must reconcile before spawning workers')
        if self._spawns or self._pending_dependents:
            raise RuntimeError('supervisor workers are already started')
        _preflight_socket_paths(self._config)
        try:
            self._restore_startup_breakers(should_stop)
            for site in self._config.sites.values():
                specs = build_worker_specs(site)
                self._specs.update((spec.worker_id, spec) for spec in specs)
                source_spec = specs[0]
                self._pending_dependents.update((spec.worker_id, spec) for spec in specs[1:])
                breaker = self._ensure_worker_breaker(source_spec)
                if breaker.is_open:
                    continue
                if (
                    breaker.restart_deadline_at is not None
                    and breaker.restart_deadline_at > self._clock()
                ):
                    self._restart_deadlines[source_spec.worker_id] = breaker.restart_deadline_at
                    continue
                self._spawn_source(source_spec)
            self._advance_worker_messages()
            self._advance_worker_messages()
        except BaseException as error:
            self._shutdown_after_failure('startup-failure', error)
            raise

    def status_mapping(
        self,
        arguments: Mapping[str, object],
    ) -> Mapping[str, object]:
        """Return aggregate health with one bounded detail projection.

        Args:
            arguments: Optional `site` and `group` selectors that narrow
                detailed records.

        Returns:
            Structured status with global liveness, readiness, and aggregate
            conditions. `projection_truncated` reports omitted whole records or
            truncated outstanding late-ID detail. `projection_counts` gives
            total and included whole-record counts for each projected section.

        Raises:
            ValueError: An argument is unsupported or a selector is invalid or
                does not identify configured state.
            RuntimeError: Supervisor status authority is unavailable.
            sqlite3.Error: Reading the supervisor audit count fails.
            OSError: Inspecting the supervisor database family fails.
        """
        if set(arguments).difference({'group', 'site'}):
            raise ValueError('status arguments are invalid')
        site_filter = arguments.get('site')
        group_filter = arguments.get('group')
        if site_filter is not None and (
            not isinstance(site_filter, str) or site_filter not in self._config.sites
        ):
            raise ValueError(f'status site {site_filter!r} is not configured; check --site')
        if group_filter is not None and not isinstance(group_filter, str):
            raise ValueError(f'status group {group_filter!r} is invalid; check --group')
        if group_filter is not None:
            matching_sites = tuple(
                site.name
                for site in self._config.sites.values()
                if group_filter in site.groups
                and (site_filter is None or site.name == site_filter)
            )
            if not matching_sites:
                raise ValueError(
                    f'status group {group_filter!r} is not configured for '
                    f'{site_filter or "any site"}; check --group'
                )

        now = self._clock()
        updates = tuple(
            self._refresh_component_health(update, now)
            for spawn in self._spawns.values()
            if (update := spawn.health) is not None
        )
        for update in updates:
            self._spawns[update.worker.worker_id].health = update
        sources = tuple(
            {
                (source.site, source.source): source
                for update in updates
                for source in update.sources
            }.values()
        )
        groups = tuple(
            {
                (group.site, group.group): group for update in updates for group in update.groups
            }.values()
        )
        workers = tuple(update.worker for update in updates)
        breaker_history = tuple(
            transition for breaker in self._breakers.values() for transition in breaker.history
        )
        state = self._state
        if state is None:
            raise RuntimeError('supervisor status authority is unavailable')
        database_sizes = (
            ('supervisor.db', state.database_size_bytes()),
            *(
                (
                    f'sites/{source.site}/sources/{source.source}.db',
                    source.database_size_bytes,
                )
                for source in sources
            ),
        )
        if self._halt_reason_value is not None:
            self._progress.set_halted()
        probe = self._progress.result(now=now)
        snapshot = aggregate_status(
            sources=sources,
            groups=groups,
            workers=workers,
            now=now,
            probe=probe,
            audit_count=(
                state.audit_count()
                + sum(source.audit_count for source in sources)
                + sum(group.audit_count for group in groups)
            ),
            database_sizes=database_sizes,
            breaker_history=breaker_history,
            breaker_transition_count=sum(
                breaker.transition_count for breaker in self._breakers.values()
            ),
        )
        payload = dict(snapshot.to_mapping())
        payload['workers'] = (
            *cast('tuple[object, ...]', payload['workers']),
            *(
                {
                    'site': self._specs[worker_id].site,
                    'worker_id': worker_id,
                    'breaker_open': True,
                    'breaker_reason': breaker.history[-1].reason,
                }
                for worker_id, breaker in self._breakers.items()
                if breaker.is_open
            ),
        )
        return _budget_status_payload(
            payload,
            group_filter=group_filter,
            site_filter=site_filter,
        )


@dataclass
class _WorkerInstance:
    """Own one spawned child through authentication, assignment, readiness, and shutdown."""

    spec: WorkerSpec
    spawn_instance: str
    _process: ChildProcess
    _control: WorkerControl
    _shutdown_deadline_secs: float
    _wait_slice_secs: float = 0.5
    _foreground_progress: Callable[[], None] | None = None
    _assigned_worker: WorkerAssignment | None = None
    _authenticated: bool = False
    _closed: bool = False
    _send_deadline_secs: float = 1.0

    def _advance_foreground_progress(self) -> None:
        """Record one completed foreground-owned bounded substep."""
        if self._foreground_progress is not None:
            self._foreground_progress()

    @staticmethod
    def _authenticate_hello(
        message: ControlMessage,
        *,
        spec: WorkerSpec,
        spawn_instance: str,
        pid: int,
    ) -> None:
        """Require the first child message to match the exact spawn."""
        if not isinstance(message, ControlHello):
            raise TypeError('worker first control message is not ControlHello')
        if (
            message.spawn_instance != spawn_instance
            or message.worker_id != spec.worker_id
            or message.pid != pid
        ):
            raise ValueError('worker hello identity conflicts with spawn request')

    @property
    def _pid(self) -> int:
        """Return the operating-system child pid."""
        return self._process.pid

    def assign(self, assignment: WorkerAssignment) -> None:
        """Deliver the exact immutable assignment once.

        A send timeout or transport failure poisons the channel; assignment is
        recorded only after a complete successful send.

        Args:
            assignment: Exact immutable assignment matching this worker and
                spawn instance.

        Raises:
            ValueError: An assignment was already delivered, its spawn identity
                conflicts, or its control frame cannot be encoded within the
                configured limit.
            RuntimeError: The child has not authenticated.
            TimeoutError: The complete send deadline expires.
            EOFError: The control channel or peer is closed.
            OSError: The control transport fails.
        """
        if self._assigned_worker is not None:
            raise ValueError('worker assignment is already delivered')
        if not self._authenticated:
            raise RuntimeError('worker must authenticate before assignment')
        if (
            assignment.worker_id != self.spec.worker_id
            or assignment.spawn_instance != self.spawn_instance
        ):
            raise ValueError('worker assignment identity conflicts with spawn')
        self._control.send(assignment, deadline_secs=self._send_deadline_secs)
        self._assigned_worker = assignment

    def authenticate(self, *, timeout: float = 0.0) -> bool:
        """Advance exact first-message authentication within one bound.

        Args:
            timeout: Maximum wait in seconds for the first complete message;
                zero only checks ready input.

        Returns:
            `True` after exact child authentication, including repeated calls;
            `False` if the complete-message wait expires.

        Raises:
            EOFError: The channel closes or its receive transport fails.
            TypeError: The first typed message is not `ControlHello`.
            ValueError: The timeout or received frame is invalid, or the
                hello's worker, spawn, or process identity conflicts.
        """
        if self._authenticated:
            return True
        try:
            message = self._control.receive(timeout=timeout)
        except TimeoutError:
            return False
        self._authenticate_hello(
            message,
            spec=self.spec,
            spawn_instance=self.spawn_instance,
            pid=self._pid,
        )
        self._authenticated = True
        return True

    def close(self) -> None:
        """Close control so the worker begins bounded graceful shutdown."""
        if not self._closed:
            self._control.close()
            self._closed = True
            self._advance_foreground_progress()

    def has_pending_input(self) -> bool:
        """Check waiting child control input without advancing its arrival clock.

        Raises:
            EOFError: The channel is closed or its receive transport fails.
        """
        return self._control.has_pending_input()

    def kill(self) -> None:
        """Force termination when the child remains alive."""
        terminate_child(self._process, force=True)
        self._advance_foreground_progress()

    @property
    def last_arrival_at(self) -> float | None:
        """Return the latest consumed control bytes' monotonic arrival."""
        return self._control.last_arrival_at

    def receive(self, *, timeout: float) -> ControlMessage:
        """Receive one typed child control message.

        Args:
            timeout: Finite nonnegative timeout in seconds for the complete
                receive operation.

        Returns:
            The next typed child control message.

        Raises:
            EOFError: The channel closes or its receive transport fails.
            TimeoutError: No complete message arrives within the timeout.
            ValueError: The peer sends an invalid frame.
        """
        return self._control.receive(timeout=timeout)

    def receive_available(self) -> Iterator[ControlMessage]:
        """Freeze the sole reader's current child control message batch.

        Acquisition occurs before iteration. The sole reader must exhaust this
        iterator before another receive operation; later-arriving bytes belong
        to a later batch.

        Returns:
            An ordered iterator over the entry message and byte snapshot.
            Earlier messages precede a later frame error or transport EOF
            raised during iteration.

        Raises:
            EOFError: The channel is closed or transport inspection fails.
            ValueError: A received frame is invalid.
        """
        return self._control.receive_available()

    def send(self, message: ControlMessage) -> None:
        """Send one typed post-assignment control message.

        A send timeout or transport failure permanently poisons the channel.

        Args:
            message: Typed control value sent after immutable assignment.

        Raises:
            ValueError: The frame cannot be encoded within the configured
                limit.
            RuntimeError: The child has not received its assignment.
            TimeoutError: The complete send deadline expires.
            EOFError: The control channel or peer is closed.
            OSError: The control transport fails.
        """
        if self._assigned_worker is None:
            raise RuntimeError('worker must receive its assignment first')
        self._control.send(message, deadline_secs=self._send_deadline_secs)

    def shutdown(self, reason: str) -> int | None:
        """Request clean stop, then kill after the configured deadline.

        Args:
            reason: Nonempty body-free reason included in the stop command.

        Returns:
            Exit status, or `None` if the child is still unreaped after
            graceful and forced-stop waits. Each wait has its own configured
            shutdown deadline; the control channel is closed before normal
            return.

        Raises:
            OSError: Force-signaling the child fails.
            Exception: Configured foreground progress reporting fails.
        """
        result = reap_child(self._process)
        self._advance_foreground_progress()
        if result is not None:
            self.close()
            return result
        with suppress(EOFError, OSError):
            self._control.send(StopCommand(reason=reason), deadline_secs=self._send_deadline_secs)
            self._advance_foreground_progress()
        result = _reap_child_until(
            self._process,
            deadline_secs=self._shutdown_deadline_secs,
            wait_slice_secs=self._wait_slice_secs,
            foreground_progress=self._foreground_progress,
        )
        if result is not None:
            self.close()
            return result
        terminate_child(self._process, force=True)
        self._advance_foreground_progress()
        result = _reap_child_until(
            self._process,
            deadline_secs=self._shutdown_deadline_secs,
            wait_slice_secs=self._wait_slice_secs,
            foreground_progress=self._foreground_progress,
        )
        self.close()
        return result

    @classmethod
    def spawn(cls, **options: Unpack[_WorkerSpawnOptions]) -> Self:
        """Spawn one unauthenticated, unassigned worker and own its control channel.

        Args:
            **options: Worker specification, nonempty spawn identity, config
                path, control-frame bound, positive startup/shutdown/wait
                durations, send deadline, and optional child environment and
                foreground progress callback.

        Returns:
            The child and control-channel owner before authentication or
            assignment. The child uses the current interpreter and inherits
            only its designated control descriptor.

        Raises:
            ValueError: Spawn identity, deadlines, or control-frame
                configuration are invalid.
            OSError: Creating the socket pair, child process, or control
                channel fails.
            RuntimeError: Failed construction leaves an unreaped child; the
                failure retains that child's exact ownership for containment.
        """
        spec = options['spec']
        spawn_instance = options['spawn_instance']
        startup_deadline_secs = options['startup_deadline_secs']
        shutdown_deadline_secs = options['shutdown_deadline_secs']
        wait_slice_secs = options.get('wait_slice_secs', shutdown_deadline_secs)
        if startup_deadline_secs <= 0 or shutdown_deadline_secs <= 0 or wait_slice_secs <= 0:
            raise ValueError('worker deadlines must be positive')
        if not spawn_instance:
            raise ValueError('worker spawn instance must not be empty')
        parent_socket, child_socket = socket.socketpair()
        control: WorkerControl | None = None
        process: ChildProcess | None = None
        try:
            control_fd = child_socket.fileno()
            process = spawn_child(
                [
                    sys.executable,
                    '-P',
                    '-m',
                    'sg_event_daemon',
                    'run-worker',
                    '--config',
                    os.fspath(options['config_path']),
                    '--site',
                    spec.site,
                    '--worker',
                    spec.worker_id,
                    '--control-fd',
                    str(control_fd),
                    '--spawn-instance',
                    spawn_instance,
                ],
                pass_fds=(control_fd,),
                env=options.get('env'),
            )
            child_socket.close()
            control = WorkerControl(
                parent_socket,
                max_frame_bytes=options['max_frame_bytes'],
            )
            instance = cls(
                spec=spec,
                spawn_instance=spawn_instance,
                _process=process,
                _control=control,
                _send_deadline_secs=options['send_deadline_secs'],
                _shutdown_deadline_secs=shutdown_deadline_secs,
                _wait_slice_secs=wait_slice_secs,
                _foreground_progress=options.get('foreground_progress'),
            )
        except BaseException as error:
            _close_failed_worker_controls(child_socket, parent_socket, control, error)
            if process is not None:
                try:
                    _cleanup_failed_worker_spawn(
                        process=process,
                        shutdown_deadline_secs=shutdown_deadline_secs,
                        wait_slice_secs=wait_slice_secs,
                        foreground_progress=options.get('foreground_progress'),
                        spec=spec,
                        spawn_instance=spawn_instance,
                    )
                except _UnreapedWorkerSpawnError as ownership_error:
                    raise ownership_error from error
                except BaseException as cleanup_error:  # noqa: BLE001
                    error.add_note(f'cleanup failure: {cleanup_error!r}')
            raise
        else:
            return instance

    def wait(self, *, timeout: float = 0.0) -> int | None:
        """Return the child status within one bounded wait.

        Args:
            timeout: Nonnegative maximum wait in seconds; zero polls without
                waiting.

        Returns:
            The child exit status, or `None` when it has not exited within the
            timeout.
        """
        return reap_child(self._process, timeout=timeout)

    @property
    def worker_id(self) -> str:
        """Return the packed worker identity."""
        return self.spec.worker_id


def _busy_operator_result(
    command: OperatorCommand,
    error: SourceDatabaseBusyError | _SupervisorDatabaseBusyError,
) -> CommandResult:
    """Preserve the owning database's busy code in a correlated response."""
    code = (
        SUPERVISOR_DATABASE_BUSY_CODE
        if isinstance(error, _SupervisorDatabaseBusyError)
        else SOURCE_DATABASE_BUSY_CODE
    )
    return CommandResult(
        command=command.command,
        ok=False,
        message=str(error),
        request_id=command.request_id,
        result={'code': code},
    )


def _close_failed_worker_controls(
    child_socket: socket.socket,
    parent_socket: socket.socket,
    control: WorkerControl | None,
    initiating: BaseException,
) -> None:
    """Release failed construction controls without losing the initiating error."""
    close_resources(
        (child_socket, parent_socket if control is None else control), primary_error=initiating
    )


def _inventory_records(
    connection: sqlite3.Connection,
) -> dict[tuple[str, str], _InventoryRecord]:
    """Return validated durable inventory rows keyed by group identity."""
    rows = connection.execute(
        """
        SELECT site, group_name, source, group_generation, orphan_generation,
               start_mode, configured, lifecycle
        FROM group_inventory
        """
    ).fetchall()
    records: dict[tuple[str, str], _InventoryRecord] = {}
    for row in rows:
        start_mode = str(row[5])
        lifecycle = str(row[7])
        if not is_start_mode(start_mode):
            raise RuntimeError('supervisor inventory start mode is invalid')
        if lifecycle not in ('active', 'orphaned', 'readded-parked'):
            raise RuntimeError('supervisor inventory lifecycle is invalid')
        records[(str(row[0]), str(row[1]))] = _InventoryRecord(
            configured=bool(row[6]),
            generation=int(row[3]),
            lifecycle=lifecycle,
            orphan_generation=None if row[4] is None else int(row[4]),
            source=str(row[2]),
            start_mode=start_mode,
        )
    return records


def _observed_generations(*values: int | None) -> tuple[int, ...]:
    """Validate and return present durable generation observations."""
    observed = tuple(value for value in values if value is not None)
    if any(type(value) is not int or value < 0 for value in observed):
        raise ValueError('durable group generations must be nonnegative integers')
    return observed


def _open_readonly_source(path: Path) -> sqlite3.Connection:
    """Open a source observation without waiting on SQLite locks."""
    return sqlite3.connect(f'{path.resolve().as_uri()}?mode=ro', uri=True, timeout=0.0)


def _preflight_socket_paths(config: Config) -> tuple[Path, ...]:
    """Validate control and source socket candidates without mutating paths."""
    try:
        control = _control.control_socket_path(config.daemon.state_dir)
    except SecurityError as error:
        raise ConfigError(f'control socket: {error}') from error
    candidates = [control]
    for site in config.sites.values():
        for source in site.sources:
            try:
                candidate = local_socket_path_candidate(
                    site_sources_directory(config, site.name),
                    source_socket_kind(source),
                    (site.name, source),
                )
            except SecurityError as error:
                raise ConfigError(f'source socket {site.name}/{source}: {error}') from error
            candidates.append(candidate)
    return tuple(candidates)


def _promote_readded_inventory(
    connection: sqlite3.Connection,
    *,
    site: str,
    group: str,
    reserved_generation: int,
    orphan_generation: int,
) -> int:
    """Promote only the configured re-added inventory with the exact reservation."""
    return connection.execute(
        """
        UPDATE group_inventory
        SET orphan_generation = NULL,
            start_mode = 'resume',
            lifecycle = 'active'
        WHERE site = ? AND group_name = ?
          AND configured = 1
          AND lifecycle = 'readded-parked'
          AND group_generation = ?
          AND orphan_generation = ?
        """,
        (site, group, reserved_generation, orphan_generation),
    ).rowcount


def _reap_child_until(
    process: ChildProcess,
    *,
    deadline_secs: float,
    wait_slice_secs: float,
    foreground_progress: Callable[[], None] | None,
) -> int | None:
    """Reap one child in bounded slices under one absolute deadline."""
    deadline = time.monotonic() + deadline_secs
    if foreground_progress is not None:
        foreground_progress()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        result = reap_child(process, timeout=min(remaining, wait_slice_secs))
        if result is not None:
            if foreground_progress is not None:
                foreground_progress()
            return result


def _cleanup_failed_worker_spawn(**options: Unpack[_FailedWorkerCleanupOptions]) -> None:
    """Reap a failed construction or expose its exact owned child handle."""
    process = options['process']
    foreground_progress = options['foreground_progress']
    errors: list[BaseException] = []
    try:
        terminate_child(process, force=True)
        if foreground_progress is not None:
            foreground_progress()
    except BaseException as error:  # noqa: BLE001
        errors.append(error)
    result: int | None = None
    try:
        result = _reap_child_until(
            process,
            deadline_secs=options['shutdown_deadline_secs'],
            wait_slice_secs=options['wait_slice_secs'],
            foreground_progress=foreground_progress,
        )
    except BaseException as error:  # noqa: BLE001
        errors.append(error)
    if result is None:
        ownership_error = _UnreapedWorkerSpawnError(
            process=process,
            spec=options['spec'],
            spawn_instance=options['spawn_instance'],
        )
        for error in errors:
            ownership_error.add_note(f'cleanup failure: {error!r}')
        raise ownership_error
    if errors:
        first_error, *later_errors = errors
        for error in later_errors:
            first_error.add_note(f'additional cleanup failure: {error!r}')
        raise first_error


def _record_generation_high_water(
    connection: sqlite3.Connection,
    *,
    site: str,
    group: str,
    generation: int,
) -> None:
    """Retain one observed generation in the supervisor high-water table."""
    connection.execute(
        """
        INSERT INTO generation_high_water (
            site, group_name, group_generation
        ) VALUES (?, ?, ?)
        ON CONFLICT (site, group_name) DO UPDATE SET
            group_generation = MAX(
                group_generation,
                excluded.group_generation
            )
        """,
        (site, group, generation),
    )


def _allocate_generation(
    connection: sqlite3.Connection,
    *,
    site: str,
    group: str,
    observed_generations: Collection[int],
) -> int:
    """Allocate above every durable or supplied generation."""
    observed = tuple(observed_generations)
    if not site or not group:
        raise ValueError('group generation identity must not be empty')
    if any(type(value) is not int or value < 0 for value in observed):
        raise ValueError('observed group generations must be nonnegative integers')
    rows = connection.execute(
        """
        SELECT group_generation
        FROM generation_high_water
        WHERE site = ? AND group_name = ?
        UNION ALL
        SELECT group_generation
        FROM group_inventory
        WHERE site = ? AND group_name = ?
        """,
        (site, group, site, group),
    ).fetchall()
    maximum = max((*observed, *(int(row[0]) for row in rows)), default=0)
    generation = maximum + 1
    _record_generation_high_water(connection, site=site, group=group, generation=generation)
    return generation


def _reconcile_active_generation(
    connection: sqlite3.Connection,
    request: _InventoryReconciliation,
) -> tuple[int, StartMode]:
    """Choose one runnable generation and exact startup mode."""
    observed = _observed_generations(
        None if request.previous is None else request.previous.generation,
        request.group_generation,
    )
    if not observed:
        return (
            _allocate_generation(
                connection,
                site=request.site,
                group=request.group,
                observed_generations=(),
            ),
            'initialize',
        )
    generation = max(observed)
    _record_generation_high_water(
        connection,
        site=request.site,
        group=request.group,
        generation=generation,
    )
    if request.group_generation is not None:
        if request.group_generation != generation:
            raise RuntimeError('greatest reconciled generation lacks matching group state')
        return generation, 'resume'
    if (
        request.previous is not None
        and request.previous.generation == generation
        and request.previous.start_mode == 'initialize'
    ):
        return generation, 'initialize'
    raise RuntimeError('resume group state is missing')


def _reconcile_parked_inventory(
    connection: sqlite3.Connection,
    request: _InventoryReconciliation,
) -> PendingDispositionSourceGroupAssignment:
    """Validate and project one still-pending re-added inventory fence."""
    previous = request.previous
    if previous is None:
        raise RuntimeError('parked reconciliation requires prior inventory')
    orphan_generation = previous.parked_orphan_generation()
    observed = _observed_generations(
        previous.generation,
        request.group_generation,
    )
    greatest = max(observed)
    _record_generation_high_water(
        connection,
        site=request.site,
        group=request.group,
        generation=greatest,
    )
    if greatest > previous.generation:
        raise RuntimeError('durable generation exceeds reserved re-add generation')
    if request.group_generation not in (
        None,
        orphan_generation,
        previous.generation,
    ):
        raise RuntimeError('group generation conflicts with re-add inventory')
    return PendingDispositionSourceGroupAssignment(
        source=request.source,
        group=request.group,
        orphan_generation=orphan_generation,
        reserved_generation=previous.generation,
    )


def _status_record_size(record: object) -> int:
    """Return exact JSON bytes added by one projected list record."""
    return len(encode_json_body({'record': record})) - len(b'{"record":}')


def _budget_status_payload(
    payload: Mapping[str, object],
    *,
    group_filter: str | None,
    site_filter: str | None,
) -> dict[str, object]:
    """Select deterministic whole detail records within the result budget."""
    sources = tuple(
        sorted(
            (
                source
                for source in cast('tuple[Mapping[str, object], ...]', payload['sources'])
                if site_filter is None or source['site'] == site_filter
            ),
            key=lambda source: (
                cast('str', source['site']),
                cast('str', source['source']),
            ),
        )
    )
    groups = tuple(
        sorted(
            (
                group
                for group in cast('tuple[Mapping[str, object], ...]', payload['groups'])
                if (site_filter is None or group['site'] == site_filter)
                and (group_filter is None or group['group'] == group_filter)
            ),
            key=lambda group: (
                cast('str', group['site']),
                cast('str', group['group']),
            ),
        )
    )
    workers = tuple(
        sorted(
            (
                worker
                for worker in cast('tuple[Mapping[str, object], ...]', payload['workers'])
                if site_filter is None or worker['site'] == site_filter
            ),
            key=lambda worker: (
                cast('str', worker['site']),
                cast('str', worker['worker_id']),
            ),
        )
    )
    database_sizes = tuple(
        sorted(
            cast('tuple[tuple[str, int], ...]', payload['database_sizes']),
            key=lambda item: item[0],
        )
    )
    breaker_history = tuple(
        sorted(
            cast('tuple[Mapping[str, object], ...]', payload['breaker_history']),
            key=lambda transition: (
                cast('str', transition['worker_id']),
                cast('int', transition['sequence']),
            ),
        )
    )
    selected: dict[str, tuple[object, ...]] = {
        'sources': sources,
        'groups': groups,
        'workers': workers,
        'database_sizes': database_sizes,
        'breaker_history': breaker_history,
    }
    included: dict[str, list[object]] = {name: [] for name in selected}
    result = dict(payload)
    for name in selected:
        result[name] = ()
    result['projection_counts'] = {
        name: {'total': len(records), 'included': len(records)}
        for name, records in selected.items()
    }
    result['projection_truncated'] = False
    encoded_size = len(encode_json_body(result))
    if encoded_size > _STATUS_RESULT_MAX_BYTES:
        raise AssertionError('empty status detail exceeds result budget')

    section_order = (
        ('groups', 'sources', 'workers', 'database_sizes', 'breaker_history')
        if group_filter is not None
        else ('sources', 'groups', 'workers', 'database_sizes', 'breaker_history')
    )
    for name in section_order:
        for record in selected[name]:
            incremental_size = _status_record_size(record) + (1 if included[name] else 0)
            if encoded_size + incremental_size > _STATUS_RESULT_MAX_BYTES:
                continue
            included[name].append(record)
            encoded_size += incremental_size

    for name, records in included.items():
        result[name] = tuple(records)
    result['projection_counts'] = {
        name: {'total': len(selected[name]), 'included': len(records)}
        for name, records in included.items()
    }
    result['projection_truncated'] = any(
        len(included[name]) < len(selected[name]) for name in selected
    ) or any(
        cast('Mapping[str, object]', group).get('outstanding_late_ids_truncated') is True
        for group in included['groups']
    )
    final_body = encode_json_body(result)
    if len(final_body) > _STATUS_RESULT_MAX_BYTES:
        raise AssertionError('projected status detail exceeds result budget')
    return result


def _store_active_inventory(
    connection: sqlite3.Connection,
    request: _InventoryReconciliation,
    generation: int,
    start_mode: StartMode,
) -> None:
    """Persist one runnable configured generation."""
    connection.execute(
        """
        INSERT INTO group_inventory (
            site, group_name, source, group_generation, orphan_generation,
            start_mode, configured, lifecycle
        ) VALUES (?, ?, ?, ?, NULL, ?, 1, 'active')
        ON CONFLICT (site, group_name) DO UPDATE SET
            source = excluded.source,
            group_generation = excluded.group_generation,
            orphan_generation = NULL,
            start_mode = excluded.start_mode,
            configured = 1,
            lifecycle = 'active'
        """,
        (
            request.site,
            request.group,
            request.source,
            generation,
            start_mode,
        ),
    )


def _reconcile_inventory_group(
    connection: sqlite3.Connection,
    request: _InventoryReconciliation,
) -> SourceGroupAssignment:
    """Reconcile one configured identity to runnable or parked inventory."""
    if not request.site or not request.source or not request.group:
        raise ValueError('configured group identity must not be empty')
    previous = request.previous
    if previous is not None and previous.source != request.source:
        raise RuntimeError(
            f'configured group {request.site}/{request.group} source changed from '
            f'{previous.source!r} to {request.source!r} without adoption'
        )
    if previous is not None and (not previous.configured or previous.lifecycle == 'orphaned'):
        orphan_generation = previous.generation
        generation = _allocate_generation(
            connection,
            site=request.site,
            group=request.group,
            observed_generations=_observed_generations(
                previous.generation,
                request.group_generation,
            ),
        )
        connection.execute(
            """
            UPDATE group_inventory
            SET group_generation = ?, start_mode = 'initialize',
                orphan_generation = ?, configured = 1,
                lifecycle = 'readded-parked'
            WHERE site = ? AND group_name = ?
            """,
            (generation, orphan_generation, request.site, request.group),
        )
        return PendingDispositionSourceGroupAssignment(
            source=request.source,
            group=request.group,
            orphan_generation=orphan_generation,
            reserved_generation=generation,
        )
    if previous is not None and previous.lifecycle == 'readded-parked':
        return _reconcile_parked_inventory(connection, request)
    generation, start_mode = _reconcile_active_generation(connection, request)
    _store_active_inventory(
        connection,
        request,
        generation=generation,
        start_mode=start_mode,
    )
    return ActiveSourceGroupAssignment(
        source=request.source,
        group=request.group,
        group_generation=generation,
        start_mode=start_mode,
    )


def _supervisor_open_file_need(sources: int, groups: int) -> int:
    """Estimate simultaneous reconciliation descriptors before the open margin."""
    return _SUPERVISOR_FIXED_DESCRIPTORS + _SUPERVISOR_SOURCE_DESCRIPTORS * sources + 5 * groups


def _supervisor_write_busy(error: DatabaseError) -> bool:
    """Distinguish SQLite BUSY from fatal database errors, including LOCKED."""
    cause = error.__cause__
    return (
        isinstance(cause, sqlite3.Error)
        and getattr(cause, 'sqlite_errorcode', 0) & 0xFF == sqlite3.SQLITE_BUSY
    )
