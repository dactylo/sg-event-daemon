"""Durable generation and deadline-driven worker supervision."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import os
import socket
import sqlite3
import sys
import time
import uuid
from collections import deque
from contextlib import ExitStack, closing, contextmanager, suppress
from dataclasses import dataclass, field, replace
from itertools import chain
from math import isfinite
from typing import TYPE_CHECKING, Literal, NotRequired, Protocol, TypedDict, Unpack, cast

from . import _control, _write_lock
from ._checkpoint import Checkpointer
from ._config import (
    WorkerConfig,
    group_state_options,
    source_state_options,
    source_stream_fingerprint,
)
from ._control import encode_json_body
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
    primary_state_for_conditions,
    probe_health_conditions,
    probe_health_ready,
)
from ._ipc import (
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
    acquire_lock,
    boot_identity,
    ensure_secure_state_dir,
    reap_child,
    release_flock,
    secure_open_regular_file,
    spawn_child,
    terminate_child,
    try_flock_exclusive,
)
from ._probe import ProbeResult, ProbeSnapshot, SupervisorProgress
from ._resources import close_resources
from ._source_state import SourceState
from ._sqlite import (
    BUSY_TIMEOUT_SECS,
    DatabaseError,
    DatabaseHandle,
    Schema,
    database_family_size,
    open_database,
    transaction,
)
from ._state_paths import (
    group_lock_path,
    site_groups_directory,
    source_database_path,
    source_lock_path,
    source_write_lock_path,
)
from ._watchdog import internal_stall_deadline_secs, watchdog_grace_secs
from ._worker import (
    WorkerSpec,
    build_worker_specs,
    stable_operator_id,
)
from ._write_lock import (
    OPERATOR_WRITE_BUDGET_SECS,
    SourceDatabaseBusyError,
    SourceWriteLock,
    WriteContext,
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
_WORKER_STALE_INTERVALS = 5
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

    _source_records: dict[str, SourceHealth]
    _group_records: dict[str, GroupHealth]
    _source_watchdogs: dict[str, WatchdogHealth]
    _lane_watchdogs: dict[str, WatchdogHealth]
    _operator_count: int


@dataclass(frozen=True)
class _InventoryRecord:
    """Project one durable configured-group inventory row."""

    _configured: bool
    _generation: int
    _lifecycle: Literal['active', 'orphaned', 'readded-parked']
    _orphan_generation: int | None
    _source: str
    _start_mode: StartMode

    def _parked_orphan_generation(self) -> int:
        """Validate the parked reservation fence and return its orphan generation."""
        orphan_generation = self._orphan_generation
        if orphan_generation is None or self._generation <= orphan_generation:
            raise RuntimeError('parked inventory generation fence is invalid')
        return orphan_generation


@dataclass(frozen=True)
class _ParkEvidence:
    """Fence one overdue lane observation to exact durable group state."""

    _evidence_id: str
    _site: str
    _group: str
    _group_generation: int
    _failed_worker_id: str
    _failed_spawn_instance: str
    _event_id: int
    _group_revision: int
    _observed_at_wall_clock: float

    def __post_init__(self) -> None:
        """Validate complete nonnegative body-free evidence."""
        if (
            not self._evidence_id
            or not self._site
            or not self._group
            or not self._failed_worker_id
            or not self._failed_spawn_instance
        ):
            raise ValueError('park evidence identity must not be empty')
        if type(self._group_generation) is not int or self._group_generation < 0:
            raise ValueError('park evidence generation must be nonnegative')
        if type(self._event_id) is not int or self._event_id < 0:
            raise ValueError('park evidence event id must be nonnegative')
        if type(self._group_revision) is not int or self._group_revision < 0:
            raise ValueError('park evidence revision must be nonnegative')
        if not isfinite(self._observed_at_wall_clock) or self._observed_at_wall_clock < 0:
            raise ValueError('park evidence observation time must be nonnegative')


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

    _group: str
    _group_generation: int | None
    _previous: _InventoryRecord | None
    _site: str
    _source: str


@dataclass(frozen=True)
class _OrphanGroupMetadata:
    """Project one absent orphan's durable group identity."""

    _group_generation: int
    _ordering: Literal['eager', 'strict']
    _site: str
    _site_server_url: str
    _source: str
    _source_generation: str
    _stream_fingerprint: str
    _verified_incarnation: str


@dataclass(frozen=True)
class _ReconciliationSourceMetadata:
    """Project one durable source identity."""

    _source_generation: str


@dataclass(frozen=True)
class _WorkerBreakerProjection:
    """Carry one complete durable worker-breaker projection."""

    _worker_id: str
    _restart_limit: int
    _restart_window_secs: float
    _healthy_reset_secs: float
    _is_open: bool
    _revision: int
    _last_failure_at: float | None
    _restart_deadline_at: float | None
    _failure_count: int
    _failure_times: tuple[float, ...]
    _transition_count: int
    _history: tuple[BreakerTransition, ...]


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
        self._failure_history: deque[BreakerTransition] = deque(maxlen=100)
        self._breaker_open = False
        self._last_failure_at: float | None = None
        self._restart_deadline: float | None = None
        self._restart_limit = restart_limit
        self._restart_window_secs = restart_window_secs
        self._breaker_revision = 0
        self._transition_total = 0
        self._worker_id = worker_id

    def _append_transition(
        self,
        state: Literal['closed', 'open'],
        reason: str,
    ) -> BreakerTransition:
        """Append one bounded transition and cumulative sequence."""
        self._transition_total += 1
        transition = BreakerTransition(
            sequence=self._transition_total,
            worker_id=self._worker_id,
            state=state,
            reason=reason,
        )
        self._failure_history.append(transition)
        return transition

    @classmethod
    def _from_projection(
        cls,
        projection: _WorkerBreakerProjection,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> Self:
        """Restore one validated complete durable projection."""
        failure_times = projection._failure_times
        history = projection._history
        if (
            type(projection._failure_count) is not int
            or projection._failure_count < len(failure_times)
            or type(projection._revision) is not int
            or projection._revision < 0
            or type(projection._transition_count) is not int
            or projection._transition_count < len(history)
            or any(not isfinite(failure_at) or failure_at < 0 for failure_at in failure_times)
            or tuple(sorted(failure_times)) != failure_times
            or (
                projection._last_failure_at is not None
                and (not isfinite(projection._last_failure_at) or projection._last_failure_at < 0)
            )
            or (
                projection._restart_deadline_at is not None
                and (
                    not isfinite(projection._restart_deadline_at)
                    or projection._restart_deadline_at < 0
                )
            )
            or (projection._is_open and projection._restart_deadline_at is not None)
            or any(transition.worker_id != projection._worker_id for transition in history)
            or any(
                history[index - 1].sequence >= history[index].sequence
                for index in range(1, len(history))
            )
            or (projection._is_open and not history)
            or (
                history
                and (
                    history[-1].sequence > projection._transition_count
                    or history[-1].state != ('open' if projection._is_open else 'closed')
                )
            )
        ):
            raise RuntimeError('durable worker breaker projection is invalid')
        breaker = cls(
            worker_id=projection._worker_id,
            restart_limit=projection._restart_limit,
            restart_window_secs=projection._restart_window_secs,
            healthy_reset_secs=projection._healthy_reset_secs,
            clock=clock,
        )
        breaker._failure_total = projection._failure_count
        breaker._failure_times.extend(failure_times)
        breaker._failure_history.extend(history)
        breaker._breaker_open = projection._is_open
        breaker._last_failure_at = projection._last_failure_at
        breaker._restart_deadline = projection._restart_deadline_at
        breaker._breaker_revision = projection._revision
        breaker._transition_total = projection._transition_count
        return breaker

    @property
    def _history(self) -> tuple[BreakerTransition, ...]:
        """Return the latest 100 open and reset transitions."""
        return tuple(self._failure_history)

    @property
    def _is_open(self) -> bool:
        """Return whether automatic restart is disabled."""
        return self._breaker_open

    def _mark_restart_started(self) -> bool:
        """Clear a consumed durable restart deadline."""
        if self._restart_deadline is None:
            return False
        self._restart_deadline = None
        return True

    @property
    def _projection(self) -> _WorkerBreakerProjection:
        """Return one complete immutable persistence projection."""
        return _WorkerBreakerProjection(
            _worker_id=self._worker_id,
            _restart_limit=self._restart_limit,
            _restart_window_secs=self._restart_window_secs,
            _healthy_reset_secs=self._healthy_reset_secs,
            _is_open=self._breaker_open,
            _revision=self._breaker_revision,
            _last_failure_at=self._last_failure_at,
            _restart_deadline_at=self._restart_deadline,
            _failure_count=self._failure_total,
            _failure_times=tuple(self._failure_times),
            _transition_count=self._transition_total,
            _history=tuple(self._failure_history),
        )

    def _record_failure(self, reason: str) -> float | None:
        """Record an unattributed failure and return its restart delay."""
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

    def _record_healthy(self) -> bool:
        """Clear closed-breaker backoff after sustained health."""
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
    def _restart_deadline_at(self) -> float | None:
        """Return the durable monotonic restart deadline."""
        return self._restart_deadline

    @property
    def _revision(self) -> int:
        """Return the compare-and-swap breaker revision."""
        return self._breaker_revision

    @property
    def _transition_count(self) -> int:
        """Return the cumulative unbounded transition count."""
        return self._transition_total

    def _validate_reset(self, *, expected_revision: int, reason: str) -> None:
        """Validate one breaker reset without mutating state."""
        if not reason:
            raise ValueError('worker breaker reset reason must not be empty')
        if len(reason.encode()) > HEALTH_TEXT_MAX_BYTES:
            raise ValueError('worker breaker reset reason exceeds 4096 UTF-8 bytes')
        if expected_revision != self._breaker_revision:
            raise ValueError('worker breaker reset revision conflicts')
        if not self._breaker_open:
            raise ValueError('worker breaker is not open')

    def _apply_audited_reset(
        self,
        *,
        expected_revision: int,
        reason: str,
    ) -> BreakerTransition:
        """Apply one already-audited compare-and-swap reset."""
        self._validate_reset(expected_revision=expected_revision, reason=reason)
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
        self._checkpointer = checkpointer_factory(
            database.connection,
            database.path,
            interval_secs=interval_secs,
            clock=clock,
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

    def _audit_count(self) -> int:
        """Return the cumulative supervisor audit count."""
        row = self._database.connection.execute('SELECT COUNT(*) FROM breaker_audit').fetchone()
        if row is None:
            raise RuntimeError('supervisor audit count is unavailable')
        return int(row[0])

    def _checkpoint(self) -> None:
        """Checkpoint the supervisor database and enforce its runtime bounds."""
        self._checkpointer.run_once()

    def _complete_group_disposition(
        self,
        *,
        site: str,
        group: str,
        orphan_generation: int,
        reserved_generation: int,
    ) -> ActiveSourceGroupAssignment:
        """Promote one worker-confirmed terminal disposition generation."""
        with transaction(self._database.connection) as connection:
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
            row = _inventory_records(connection)[(site, group)]
        return ActiveSourceGroupAssignment(
            source=row._source,
            group=group,
            group_generation=reserved_generation,
            start_mode='resume',
        )

    def _complete_orphan_disposition(
        self,
        *,
        site: str,
        group: str,
        orphan_generation: int,
        reserved_generation: int,
    ) -> None:
        """Retain one terminal absent group at its disposed generation."""
        with transaction(self._database.connection) as connection:
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

    def _configured_group_assignments(
        self,
    ) -> dict[tuple[str, str], SourceGroupAssignment]:
        """Project current durable configured inventory into closed assignments."""
        assignments: dict[tuple[str, str], SourceGroupAssignment] = {}
        for identity, record in _inventory_records(self._database.connection).items():
            if not record._configured:
                continue
            if record._lifecycle == 'active':
                if record._orphan_generation is not None:
                    raise RuntimeError('active inventory retains an orphan generation')
                assignment: SourceGroupAssignment = ActiveSourceGroupAssignment(
                    source=record._source,
                    group=identity[1],
                    group_generation=record._generation,
                    start_mode=record._start_mode,
                )
            elif record._lifecycle == 'readded-parked':
                orphan_generation = record._parked_orphan_generation()
                assignment = PendingDispositionSourceGroupAssignment(
                    source=record._source,
                    group=identity[1],
                    orphan_generation=orphan_generation,
                    reserved_generation=record._generation,
                )
            else:
                raise RuntimeError('configured inventory lifecycle is invalid')
            assignments[identity] = assignment
        return assignments

    def _database_size_bytes(self) -> int:
        """Return current bytes used by the supervisor SQLite family."""
        return database_family_size(self._database.path)

    def _generation_high_water(self, site: str, group: str) -> int:
        """Read the durable transition identity retained across pending removals."""
        row = self._database.connection.execute(
            'SELECT group_generation FROM generation_high_water WHERE site = ? AND group_name = ?',
            (site, group),
        ).fetchone()
        if row is None:
            raise RuntimeError('group generation high-water is unavailable')
        return int(row[0])

    def _group_inventory_record(
        self,
        *,
        site: str,
        group: str,
    ) -> _InventoryRecord | None:
        """Return one exact durable inventory row."""
        return _inventory_records(self._database.connection).get((site, group))

    def _inventory_records(self) -> dict[tuple[str, str], _InventoryRecord]:
        """Return every validated durable group inventory row."""
        return _inventory_records(self._database.connection)

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
        with transaction(self._database.connection) as connection:
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

    @classmethod
    def _open(
        cls,
        state_dir: Path,
        *,
        synchronous: Literal['normal', 'full'] = 'normal',
        **options: Unpack[_SupervisorCheckpointOptions],
    ) -> Self:
        """Open or initialize durable supervisor state."""
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

    def _orphaned_groups(self) -> dict[tuple[str, str], int]:
        """Return every omitted group and its retired generation."""
        rows = self._database.connection.execute(
            """
            SELECT site, group_name, group_generation
            FROM group_inventory
            WHERE configured = 0 AND lifecycle = 'orphaned'
            ORDER BY site, group_name
            """
        ).fetchall()
        return {(str(row[0]), str(row[1])): int(row[2]) for row in rows}

    def _pending_park_evidence(self) -> tuple[_ParkEvidence, ...]:
        """Return every durable pre-termination lane observation."""
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
                _evidence_id=str(row[0]),
                _site=str(row[1]),
                _group=str(row[2]),
                _group_generation=int(row[3]),
                _failed_worker_id=str(row[4]),
                _failed_spawn_instance=str(row[5]),
                _event_id=int(row[6]),
                _group_revision=int(row[7]),
                _observed_at_wall_clock=float(row[8]),
            )
            for row in rows
        )

    def _persist_worker_breaker(self, breaker: _WorkerBreaker) -> None:
        """Replace one complete durable breaker projection atomically."""
        projection = breaker._projection
        with transaction(self._database.connection) as connection:
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
                    projection._worker_id,
                    projection._restart_limit,
                    projection._restart_window_secs,
                    projection._healthy_reset_secs,
                    projection._is_open,
                    projection._revision,
                    projection._last_failure_at,
                    projection._restart_deadline_at,
                    projection._failure_count,
                    projection._transition_count,
                ),
            )
            connection.execute(
                'DELETE FROM worker_breaker_failures WHERE worker_id = ?',
                (projection._worker_id,),
            )
            connection.executemany(
                """
                INSERT INTO worker_breaker_failures (
                    worker_id, failure_index, failure_at
                ) VALUES (?, ?, ?)
                """,
                (
                    (projection._worker_id, index, failure_at)
                    for index, failure_at in enumerate(projection._failure_times)
                ),
            )
            connection.execute(
                'DELETE FROM worker_breaker_transitions WHERE worker_id = ?',
                (projection._worker_id,),
            )
            connection.executemany(
                """
                INSERT INTO worker_breaker_transitions (
                    worker_id, sequence, state, reason
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    (
                        projection._worker_id,
                        transition.sequence,
                        transition.state,
                        transition.reason,
                    )
                    for transition in projection._history
                ),
            )

    def _promote_readded_group(
        self,
        *,
        site: str,
        group: str,
        source_state: SourceState,
        group_state: GroupState,
    ) -> bool:
        """Promote one re-add only after exact group and source terminal proof."""
        with transaction(self._database.connection) as connection:
            record = _inventory_records(connection).get((site, group))
            if record is None or record._lifecycle != 'readded-parked':
                return False
            orphan_generation = record._parked_orphan_generation()
            source = source_state.snapshot()
            group_snapshot = group_state.snapshot()
            if (
                source.site != site
                or source.source != record._source
                or group_snapshot.site != site
                or group_snapshot.source != record._source
                or group_snapshot.group != group
            ):
                raise RuntimeError('re-add promotion authority identity conflicts')
            identity = group_state.terminal_disposition_identity(record._generation)
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
                reserved_generation=record._generation,
                orphan_generation=orphan_generation,
            )
            return True

    def _reconcile_groups(
        self,
        *,
        configured: Mapping[tuple[str, str], str],
        generations: Mapping[tuple[str, str], int],
    ) -> dict[tuple[str, str], SourceGroupAssignment]:
        """Reconcile configured groups before any worker spawn."""
        assignments: dict[tuple[str, str], SourceGroupAssignment] = {}
        with transaction(self._database.connection) as connection:
            configured_keys = set(configured)
            stored = _inventory_records(connection)
            for (site, group), source in sorted(configured.items()):
                assignments[(site, group)] = _reconcile_inventory_group(
                    connection,
                    _InventoryReconciliation(
                        _group=group,
                        _group_generation=generations.get((site, group)),
                        _previous=stored.get((site, group)),
                        _site=site,
                        _source=source,
                    ),
                )
            for site, group in set(stored).difference(configured_keys):
                previous = stored[(site, group)]
                if previous._orphan_generation is not None and (
                    not previous._configured
                    or generations.get((site, group)) == previous._generation
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

    def _record_park_evidence(self, evidence: _ParkEvidence) -> bool:
        """Insert evidence only when its group has no unresolved row."""
        with transaction(self._database.connection) as connection:
            existing = connection.execute(
                """
                SELECT 1
                FROM park_evidence
                WHERE site = ? AND group_name = ?
                """,
                (evidence._site, evidence._group),
            ).fetchone()
            if existing is not None:
                return False
            connection.execute(
                """
                INSERT INTO park_evidence (
                    evidence_id, site, group_name, group_generation,
                    failed_worker_id, failed_spawn_instance, event_id,
                    group_revision, observed_at_wall_clock
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evidence._evidence_id,
                    evidence._site,
                    evidence._group,
                    evidence._group_generation,
                    evidence._failed_worker_id,
                    evidence._failed_spawn_instance,
                    evidence._event_id,
                    evidence._group_revision,
                    evidence._observed_at_wall_clock,
                ),
            )
        return True

    def _reserve_group_disposition(
        self,
        *,
        site: str,
        group: str,
        observed_generation: int,
    ) -> PendingDispositionSourceGroupAssignment:
        """Reserve or replay one durable old-to-new operator generation fence."""
        with transaction(self._database.connection) as connection:
            record = _inventory_records(connection).get((site, group))
            if record is None:
                raise ValueError('group disposition target is unknown')
            if not record._configured:
                if record._lifecycle != 'orphaned':
                    raise RuntimeError('absent group disposition inventory is invalid')
                orphan_generation = record._orphan_generation
                if orphan_generation is not None:
                    return PendingDispositionSourceGroupAssignment(
                        source=record._source,
                        group=group,
                        orphan_generation=orphan_generation,
                        reserved_generation=record._generation,
                    )
                reserved_generation = _allocate_generation(
                    connection,
                    site=site,
                    group=group,
                    observed_generations=_observed_generations(
                        record._generation, observed_generation
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
                        record._generation,
                        site,
                        group,
                        record._generation,
                    ),
                )
                return PendingDispositionSourceGroupAssignment(
                    source=record._source,
                    group=group,
                    orphan_generation=record._generation,
                    reserved_generation=reserved_generation,
                )
            if record._lifecycle == 'readded-parked':
                orphan_generation = record._orphan_generation
                if orphan_generation is None:
                    raise RuntimeError('pending disposition lacks an orphan generation')
                return PendingDispositionSourceGroupAssignment(
                    source=record._source,
                    group=group,
                    orphan_generation=orphan_generation,
                    reserved_generation=record._generation,
                )
            if record._lifecycle != 'active' or record._orphan_generation is not None:
                raise RuntimeError('group disposition inventory is not active')
            reserved_generation = _allocate_generation(
                connection,
                site=site,
                group=group,
                observed_generations=_observed_generations(
                    record._generation, observed_generation
                ),
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
                    record._generation,
                    site,
                    group,
                    record._generation,
                ),
            )
            return PendingDispositionSourceGroupAssignment(
                source=record._source,
                group=group,
                orphan_generation=record._generation,
                reserved_generation=reserved_generation,
            )

    def _reset_worker_breaker(
        self,
        breaker: _WorkerBreaker,
        *,
        expected_revision: int,
        reason: str,
        identity: AuditIdentity,
        wall_clock_time: float,
    ) -> BreakerTransition:
        """Durably audit and apply one exact worker-breaker reset."""
        if not isfinite(wall_clock_time) or wall_clock_time < 0:
            raise ValueError('worker breaker audit time must be nonnegative')
        breaker._validate_reset(
            expected_revision=expected_revision,
            reason=reason,
        )
        transition_sequence = breaker._transition_count + 1
        with transaction(self._database.connection) as connection:
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
                    breaker._worker_id,
                    expected_revision,
                    breaker._transition_count,
                ),
            )
            if updated.rowcount != 1:
                raise ValueError('worker breaker reset revision conflicts')
            connection.execute(
                'DELETE FROM worker_breaker_failures WHERE worker_id = ?',
                (breaker._worker_id,),
            )
            connection.execute(
                """
                INSERT INTO worker_breaker_transitions (
                    worker_id, sequence, state, reason
                ) VALUES (?, ?, 'closed', ?)
                """,
                (breaker._worker_id, transition_sequence, reason),
            )
            connection.execute(
                """
                DELETE FROM worker_breaker_transitions
                WHERE worker_id = ? AND sequence <= ?
                """,
                (breaker._worker_id, transition_sequence - 100),
            )
            connection.execute(
                """
                INSERT INTO breaker_audit (
                    worker_id, prior_revision, new_revision, command, reason,
                    wall_clock_time, local_uid, username, pid
                ) VALUES (?, ?, ?, 'reset-worker-breaker', ?, ?, ?, ?, ?)
                """,
                (
                    breaker._worker_id,
                    expected_revision,
                    expected_revision + 1,
                    reason,
                    wall_clock_time,
                    identity.local_uid,
                    identity.username,
                    identity.pid,
                ),
            )
        return breaker._apply_audited_reset(
            expected_revision=expected_revision,
            reason=reason,
        )

    def _restore_worker_breaker(
        self,
        **options: Unpack[_WorkerBreakerRestoreOptions],
    ) -> _WorkerBreaker:
        """Restore one complete breaker or create a fresh configured one."""
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
        restored = _WorkerBreaker._from_projection(
            _WorkerBreakerProjection(
                _worker_id=worker_id,
                _restart_limit=restart_limit,
                _restart_window_secs=restart_window_secs,
                _healthy_reset_secs=healthy_reset_secs,
                _is_open=bool(row[3]),
                _revision=int(row[4]),
                _last_failure_at=None if row[5] is None else float(row[5]),
                _restart_deadline_at=None if row[6] is None else float(row[6]),
                _failure_count=int(row[7]),
                _failure_times=tuple(float(failure[1]) for failure in failures),
                _transition_count=int(row[8]),
                _history=tuple(transitions),
            ),
            clock=clock,
        )
        if (
            int(row[0]) != restart_limit
            or float(row[1]) != restart_window_secs
            or float(row[2]) != healthy_reset_secs
        ):
            self._persist_worker_breaker(restored)
        return restored

    def _retire_park_evidence(self, evidence_id: str) -> None:
        """Delete one exact acknowledged evidence row."""
        with transaction(self._database.connection) as connection:
            deleted = connection.execute(
                'DELETE FROM park_evidence WHERE evidence_id = ?',
                (evidence_id,),
            )
            if deleted.rowcount != 1:
                raise RuntimeError('conditional park evidence changed before retirement')

    def close(self) -> None:
        """Close the supervisor database."""
        self._database.close()


class _WorkerHandle(Protocol):
    """Define the spawned worker surface advanced through staged startup."""

    _spawn_instance: str
    _spec: WorkerSpec

    def _assign(self, assignment: WorkerAssignment) -> None:
        """Send the worker's single immutable assignment."""
        ...

    def _authenticate(self, *, timeout: float = 0.0) -> bool:
        """Advance exact child authentication without exceeding `timeout`."""
        ...

    def _close(self) -> None:
        """Close the worker control channel."""
        ...

    def _kill(self) -> None:
        """Force termination without waiting for worker cleanup."""
        ...

    def _receive(self, *, timeout: float) -> ControlMessage:
        """Receive one typed worker message."""
        ...

    def _receive_available(self) -> Iterator[ControlMessage]:
        """Freeze one finite ordered batch for the sole channel reader."""
        ...

    def _send(self, message: ControlMessage) -> None:
        """Send one typed post-assignment control message."""
        ...

    def _shutdown(self, reason: str) -> int | None:
        """Stop and reap the worker within its deadline."""
        ...

    def _wait(self, *, timeout: float = 0.0) -> int | None:
        """Reap the worker only when it has already exited."""
        ...

    @property
    def _worker_id(self) -> str:
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
    """Carry one startup control-channel operating-system failure."""

    def __init__(
        self,
        worker: _WorkerHandle,
        error: OSError | EOFError | ValueError,
        *,
        sending: bool,
    ) -> None:
        """Retain the affected worker, failure, and send-phase flag."""
        self._error = error
        self._sending = sending
        self._worker = worker


@dataclass
class _WorkerLifecycle:
    """Track one deadline-bounded worker startup and recovery fence."""

    _assignment: WorkerAssignment
    _deadline_at: float
    _worker: _WorkerHandle
    _assigned: bool = False
    _authenticated: bool = False
    _endpoint_messages: dict[str, SourceEndpointReady] = field(
        default_factory=dict[str, SourceEndpointReady]
    )
    _grant_index: int = 0
    _grant_sent: bool = False
    _pending_recovery: dict[str, ConditionalParkDirective] = field(
        default_factory=dict[str, ConditionalParkDirective]
    )
    _ready: bool = False

    def _active_grant(self) -> EventPeerGrant | None:
        """Return the sent grant still awaiting the source owner's reply."""
        grants = self._assignment.event_grants
        if not self._grant_sent or self._grant_index >= len(grants):
            return None
        return grants[self._grant_index]


@dataclass
class _WorkerSpawn:
    """Own all ephemeral state for one exact worker process."""

    _worker: _WorkerHandle
    _health_deadline: float
    _pending_groups: frozenset[str]
    _lifecycle: _WorkerLifecycle | None
    _attributed_park_failure: bool = False
    _health: HealthUpdate | None = None
    _health_failure: str | None = None
    _health_inventory_complete: bool = False
    _termination_reason: str | None = None


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
                opaque lifecycle identities.
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
        self._halt_reason_value: str | None = None
        self._message_batches: dict[str, Iterator[ControlMessage]] | None = None
        self._batch_message_count = 0
        self._lock: FileLock | None = None
        self._next_checkpoint_at: float | None = None
        self._checkpointer_factory = options.get('checkpointer_factory', Checkpointer)
        self._operator_request_sequence = 0
        self._park_evidence_by_group: dict[tuple[str, str], _ParkEvidence] = {}
        self._pending_dependents: dict[str, WorkerSpec] = {}
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
        self._worker_factory = _WorkerInstance._spawn if worker_factory is None else worker_factory
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
            ValueError: The supervisor clock is not finite and nonnegative.
        """
        self._open()
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Close workers and release the state-dir lock."""
        self._close()

    def _active_dependent_grant_owner(
        self,
        source_worker: _WorkerHandle,
    ) -> _WorkerLifecycle | None:
        """Return the lifecycle currently consuming one source control channel."""
        for spawn in self._spawns.values():
            lifecycle = spawn._lifecycle
            if lifecycle is None:
                continue
            grant = lifecycle._active_grant()
            if grant is None:
                continue
            owner_worker = self._source_workers.get((lifecycle._worker._spec.site, grant.source))
            if owner_worker is source_worker:
                return lifecycle
        return None

    def _advance_progress(self) -> None:
        """Record one completed bounded foreground step."""
        self._progress.advance(now=self._clock())

    def _complete_worker_lifecycle(
        self,
        lifecycle: _WorkerLifecycle,
    ) -> None:
        """Release a ready lifecycle only after every recovery result."""
        if lifecycle._ready and not lifecycle._pending_recovery:
            self._spawns[lifecycle._worker._worker_id]._lifecycle = None

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
            record._orphan_generation is None
            and group_state.lifecycle() == 'active'
            and self._current_disposition_proof(source_state, group_state, record._generation)
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
            self._breakers[spec.worker_id] = state._restore_worker_breaker(
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
            lifecycle = spawn._lifecycle
            if lifecycle is None:
                continue
            grant = lifecycle._active_grant()
            if grant is None:
                continue
            source_worker = self._source_workers.get((lifecycle._worker._spec.site, grant.source))
            if source_worker is None:
                if self._spawns[worker_id]._termination_reason is not None:
                    continue
                raise RuntimeError('active dependent grant source owner is unavailable')
            worker_ids.add(source_worker._worker_id)
        return worker_ids

    @staticmethod
    def _group_disposition_target(command: OperatorCommand) -> tuple[str, str]:
        """Validate and return one exact group disposition target."""
        expected = {'group', 'reason', 'site'}
        if command.command == 'replay':
            expected.update({'acknowledge_truncation', 'from_event_id'})
        elif command.command != 'reset-group-to-latest':
            raise ValueError('group disposition command is invalid')
        if set(command.arguments) != expected:
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
                None if state is None else state._group_inventory_record(site=site, group=group)
            )
            if record is None:
                raise RuntimeError('absent orphan group inventory is missing')
            source = record._source
        return source_database_path(self._config, site, source)

    def _disposition_observation(
        self, site: str, group: str, disposition_id: str
    ) -> tuple[tuple[int, int] | None, int, bool]:
        """Read receipt, revision, and command admission without mutating source state."""
        path = self._group_source_path(site, group)
        with closing(_open_readonly_source(path)) as connection:
            row = connection.execute(
                'SELECT old_generation, new_generation FROM disposition_audit '
                'WHERE group_name = ? AND disposition_id = ?',
                (group, disposition_id),
            ).fetchone()
            revision = connection.execute(
                'SELECT revision, parked, lifecycle FROM group_state '
                'JOIN groups USING (group_name) '
                'WHERE group_name = ?',
                (group,),
            ).fetchone()
            if revision is None:
                raise RuntimeError('group disposition state is unavailable')
            return (
                None if row is None else (int(row[0]), int(row[1])),
                int(revision[0]),
                disposition_admitted(parked=bool(revision[1]), lifecycle=str(revision[2])),
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
                row = connection.execute(
                    """
                    SELECT source_meta.site, groups.group_name, groups.group_generation
                    FROM groups CROSS JOIN source_meta
                    JOIN group_state ON group_state.group_name = groups.group_name
                    WHERE groups.group_name = ?
                    """,
                    (assignment.group,),
                ).fetchone()
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

    @staticmethod
    def _group_state_exists(path: Path, group: str) -> bool:
        """Check the group row in an existing source without creating a file."""
        if not path.is_file():
            return False
        connection = _open_readonly_source(path)
        try:
            return (
                connection.execute(
                    'SELECT 1 FROM groups WHERE group_name = ?', (group,)
                ).fetchone()
                is not None
            )
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

    def _next_monitor_deadline(self) -> float:
        """Return the earliest active monitor deadline."""
        next_checkpoint_at = self._next_checkpoint_at
        if next_checkpoint_at is None:
            raise RuntimeError('supervisor is not open')
        deadlines = [
            next_checkpoint_at,
            *(spawn._health_deadline for spawn in self._spawns.values()),
        ]
        deadlines.extend(
            lifecycle._deadline_at
            for spawn in self._spawns.values()
            if (lifecycle := spawn._lifecycle) is not None
        )
        for worker_id, deadline in self._restart_deadlines.items():
            if worker_id in self._spawns:
                continue
            spec = self._specs[worker_id]
            if spec.sources or self._dependent_sources_ready(spec):
                deadlines.append(deadline)
        return min(deadlines)

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
        if evidence is not None and evidence._group_generation not in generations:
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
                None if state is None else state._configured_group_assignments().get((site, group))
            )
            if isinstance(
                assignment,
                PendingDispositionSourceGroupAssignment,
            ):
                raise ValueError('group cannot unpark before its source-adoption disposition')
        owners = tuple(
            worker
            for spawn in self._spawns.values()
            for worker in (spawn._worker,)
            if worker._spec.site == site
            and self._spawns[worker._worker_id]._lifecycle is None
            and self._spawns[worker._worker_id]._termination_reason is None
            and worker._worker_id not in self._grant_worker_ids()
            and (
                (
                    isinstance(source, str)
                    and (group is None or command.command == 'group-disposition')
                    and source in worker._spec.sources
                )
                or (isinstance(group, str) and source is None and group in worker._spec.groups)
            )
        )
        if len(owners) != 1:
            raise ValueError('operator command does not have one running owner')
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
            row = connection.execute(
                """
                SELECT group_generation, ordering_mode, site, site_server_url,
                       source_name, source_generation, stream_fingerprint,
                       stream_incarnation
                FROM groups CROSS JOIN source_meta
                WHERE group_name = ?
                """,
                (group,),
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise RuntimeError('absent orphan group metadata is missing')
        ordering = str(row[1])
        if ordering not in {'eager', 'strict'}:
            raise RuntimeError('absent orphan ordering is invalid')
        return _OrphanGroupMetadata(
            _group_generation=int(row[0]),
            _ordering=cast("Literal['eager', 'strict']", ordering),
            _site=str(row[2]),
            _site_server_url=str(row[3]),
            _source=str(row[4]),
            _source_generation=str(row[5]),
            _stream_fingerprint=str(row[6]),
            _verified_incarnation=str(row[7]),
        )

    def _pending_worker_groups(self, worker: _WorkerHandle) -> set[str]:
        """Return source-owned status records absent from this worker's lane inventory."""
        spawn = self._spawns.get(worker._worker_id)
        if spawn is None or spawn._worker is not worker:
            return set()
        return set(spawn._pending_groups)

    def _inventory_failure_reason(
        self,
        worker: _WorkerHandle,
        inventory: _HealthInventory,
    ) -> str | None:
        """Return the reason for one incomplete configured inventory."""
        expected_sources = set(worker._spec.sources)
        expected_groups = set(worker._spec.groups)
        if (
            set(inventory._source_records) != expected_sources
            or set(inventory._source_watchdogs) != expected_sources
        ):
            return 'source-phase-overdue'
        if (
            set(inventory._group_records) != expected_groups | self._pending_worker_groups(worker)
            or set(inventory._lane_watchdogs) != expected_groups
        ):
            return 'lane-phase-overdue'
        if inventory._operator_count != 1:
            return 'operator-phase-overdue'
        return None

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
        """Identify operating-system failures from an exact control operation."""
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
        if group_state.lifecycle() != 'orphaned':
            group_state.change_lifecycle(
                LifecycleRequest(
                    command='orphan',
                    expected_lifecycle=cast(
                        "Literal['active', 'readded-parked']", group_state.lifecycle()
                    ),
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

    def _overdue_group_health(
        self,
        worker: _WorkerHandle,
    ) -> tuple[GroupHealth, ...]:
        """Return lanes confirmed overdue by their accepted frame timestamp."""
        update = self._spawns[worker._spec.worker_id]._health
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
        update = self._spawns[worker._spec.worker_id]._health
        reconciled = self._reconciled
        assignment = (
            None if reconciled is None else reconciled.get((worker._spec.site, group.group))
        )
        event_id = group.current_event_id
        path = self._group_source_path(worker._spec.site, group.group)
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
                row = connection.execute(
                    """
                    SELECT groups.group_generation, group_state.revision,
                           group_state.in_flight_event_id, group_state.parked
                    FROM group_state
                    JOIN groups ON groups.group_name = group_state.group_name
                    WHERE group_state.group_name = ?
                    """,
                    (group.group,),
                ).fetchone()
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
            _evidence_id=self._next_token(),
            _site=worker._spec.site,
            _group=group.group,
            _group_generation=row[0],
            _failed_worker_id=worker._worker_id,
            _failed_spawn_instance=worker._spawn_instance,
            _event_id=event_id,
            _group_revision=row[1],
            _observed_at_wall_clock=self._wall_clock(),
        )

    @staticmethod
    def _park_directive(evidence: _ParkEvidence) -> ConditionalParkDirective:
        """Project one durable evidence row into its lane-owned directive."""
        return ConditionalParkDirective(
            evidence_id=evidence._evidence_id,
            site=evidence._site,
            group=evidence._group,
            group_generation=evidence._group_generation,
            failed_spawn_instance=evidence._failed_spawn_instance,
            event_id=evidence._event_id,
            group_revision=evidence._group_revision,
            observed_at_wall_clock=evidence._observed_at_wall_clock,
        )

    def _accept_group_recovery_ready(
        self,
        lifecycle: _WorkerLifecycle,
        message: GroupRecoveryReady,
    ) -> None:
        """Retire exact durable evidence from its authenticated current spawn."""
        worker = lifecycle._worker
        if (
            not lifecycle._authenticated
            or not lifecycle._assigned
            or (spawn := self._spawns.get(worker._worker_id)) is None
            or spawn._worker is not worker
            or lifecycle._assignment.spawn_instance != worker._spawn_instance
            or message.spawn_instance != worker._spawn_instance
        ):
            raise ValueError('group recovery ready spawn is not authenticated current authority')
        directive = lifecycle._pending_recovery.get(message.evidence_id)
        if directive is None:
            raise ValueError('group recovery ready is duplicate or conflicts')
        if (
            message.site != directive.site
            or message.site != worker._spec.site
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
        if evidence is None or self._park_directive(evidence) != directive:
            raise ValueError('group recovery ready evidence conflicts with current authority')
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        durable = next(
            (
                item
                for item in state._pending_park_evidence()
                if item._evidence_id == directive.evidence_id
            ),
            None,
        )
        if durable is None or self._park_directive(durable) != directive:
            raise ValueError('group recovery ready durable evidence is unavailable')
        state._retire_park_evidence(directive.evidence_id)
        self._park_evidence_by_group.pop((directive.site, directive.group))
        lifecycle._pending_recovery.pop(directive.evidence_id)

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
        reconciled = state._configured_group_assignments()
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
                if evidence._group_generation != inventory.group_generation:
                    raise RuntimeError(
                        f'conditional park evidence generation conflicts: {site.name}/{group_name}'
                    )
                directive = self._park_directive(evidence)
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
        for (site, group), record in state._inventory_records().items():
            if not record._configured or (site, group) in configured:
                continue
            group_state = group_states.get((site, group))
            source_state = source_states.get((site, record._source))
            if group_state is None or source_state is None:
                raise RuntimeError(f'omitted group state is unavailable: {site}/{group}')
            snapshot = group_state.snapshot()
            if (
                snapshot.site != site
                or snapshot.group != group
                or snapshot.source != record._source
                or snapshot.group_generation not in {record._generation, record._orphan_generation}
            ):
                raise RuntimeError(f'omitted group authority conflicts: {site}/{group}')
            self._omitted_park_evidence(
                site, group, (record._generation, record._orphan_generation)
            )
            self._check_source_incarnation(source_state, group_state)
            self._orphan_group(
                source_state,
                group_state,
                transition_generation=state._generation_high_water(site, group),
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
                state._promote_readded_group(
                    site=site_name,
                    group=group_name,
                    source_state=source_state,
                    group_state=group_state,
                )
                or promoted
            )
        return promoted, current_generations

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

    def _receive_worker_message(self, worker: _WorkerHandle) -> ControlMessage:
        """Read the current owner's finite batch without reacquiring on handoff."""
        batches = self._message_batches
        if batches is None:
            return worker._receive(timeout=0.0)
        if worker._spawn_instance not in batches:
            batches[worker._spawn_instance] = worker._receive_available()
        try:
            message = next(batches[worker._spawn_instance])
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
        records = state._inventory_records()
        for (site, group), generation in sorted(state._orphaned_groups().items()):
            group_state = group_states.get((site, group))
            if group_state is None:
                raise RuntimeError(f'omitted group state is unavailable: {site}/{group}')
            group_snapshot = group_state.snapshot()
            record = records[(site, group)]
            if (
                group_snapshot.site != site
                or group_snapshot.group != group
                or group_snapshot.group_generation not in {generation, record._orphan_generation}
            ):
                raise RuntimeError(f'omitted group authority conflicts: {site}/{group}')
            evidence = self._omitted_park_evidence(
                site, group, (generation, record._orphan_generation)
            )
            source_state = source_states.get((site, group_snapshot.source))
            if source_state is None:
                raise RuntimeError(
                    f'omitted group source state is unavailable: {site}/{group_snapshot.source}'
                )
            self._check_source_incarnation(source_state, group_state)
            if (
                record._orphan_generation is not None
                and group_snapshot.group_generation == generation
            ):
                self._check_source_incarnation(
                    source_state,
                    group_state,
                )
                if not self._current_disposition_proof(source_state, group_state, generation):
                    raise RuntimeError(f'omitted group terminal is unavailable: {site}/{group}')
                state._complete_orphan_disposition(
                    site=site,
                    group=group,
                    orphan_generation=record._orphan_generation,
                    reserved_generation=generation,
                )
            elif not self._completed_absent_disposition(record, source_state, group_state):
                self._orphan_group(
                    source_state,
                    group_state,
                    transition_generation=state._generation_high_water(site, group),
                )
            if evidence is None:
                continue
            state._retire_park_evidence(evidence._evidence_id)
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
            row = connection.execute(
                """
                SELECT source_generation
                FROM source_meta
                WHERE singleton = 1
                """
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise RuntimeError('durable source metadata is missing')
        return _ReconciliationSourceMetadata(
            _source_generation=str(row[0]),
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
            if (update := spawn._health) is not None
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
                self._spawns[worker_id]._health_inventory_complete
                for worker_id in expected_worker_ids
            )
            and expected_sources == {(source.site, source.source) for source in sources}
            and expected_groups == {(group.site, group.group) for group in groups}
        )
        stale_after_secs = self._config.daemon.health_interval_secs * _WORKER_STALE_INTERVALS
        worker_breaker_open = any(breaker._is_open for breaker in self._breakers.values())
        conditions = probe_health_conditions(
            sources=sources,
            groups=groups,
            workers=workers,
            now=now,
            stale_after_secs=stale_after_secs,
            worker_breaker_open=worker_breaker_open,
            inventory_complete=complete,
        )
        ready = probe_health_ready(
            sources=sources,
            groups=groups,
            workers=workers,
            now=now,
            stale_after_secs=stale_after_secs,
            worker_breaker_open=worker_breaker_open,
            inventory_complete=complete,
        )
        return ProbeSnapshot(
            generated_at=now,
            ready=ready,
            complete=complete,
            conditions=conditions,
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
            _worker=worker,
            _health_deadline=0.0,
            _pending_groups=frozenset(),
            _lifecycle=None,
        )
        # Own the child before fallible clock and recovery initialization.
        self._spawns[spec.worker_id] = spawn
        spawn._pending_groups = frozenset(
            item.group
            for item in assignment.source_group_assignments
            if isinstance(item, PendingDispositionSourceGroupAssignment)
            and item.source in spec.sources
        )
        deadline = self._clock() + self._config.daemon.worker_startup_deadline_secs
        spawn._health_deadline = deadline
        spawn._lifecycle = _WorkerLifecycle(
            _assignment=assignment,
            _deadline_at=deadline,
            _pending_recovery=self._pending_recovery(assignment),
            _worker=worker,
        )
        return worker

    def _register_worker(self, worker: _WorkerHandle) -> None:
        """Register one ready spawn without replacing its breaker history."""
        spec = worker._spec
        breaker = self._ensure_worker_breaker(spec)
        breaker._mark_restart_started()
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        state._persist_worker_breaker(breaker)
        self._restart_deadlines.pop(spec.worker_id, None)

    def _retire_dependent_grant(self, worker: _WorkerHandle) -> None:
        """Release a retiring dependent's read ownership and fence its late reply."""
        lifecycle = self._spawns[worker._worker_id]._lifecycle
        grant = None if lifecycle is None else lifecycle._active_grant()
        if lifecycle is not None and grant is not None:
            self._stale_grant_readies.add(
                EventPeerReady(
                    source=grant.source,
                    group=grant.group,
                    dependent_spawn_instance=grant.dependent_spawn_instance,
                )
            )
            lifecycle._grant_sent = False

    def _discard_worker_lifecycle(self, worker: _WorkerHandle) -> None:
        """Retire every ephemeral projection owned by one exact worker."""
        worker_id = worker._worker_id
        if (spawn := self._spawns.get(worker_id)) is None or spawn._worker is not worker:
            raise RuntimeError('worker lifecycle owner conflicts with current spawn')
        lifecycle = self._spawns[worker_id]._lifecycle
        if lifecycle is not None and lifecycle._worker is not worker:
            raise RuntimeError('worker startup lifecycle owner conflicts with current spawn')
        self._retire_dependent_grant(worker)
        try:
            worker._close()
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
            (spawn := self._spawns.get(worker._worker_id)) is None
            or spawn._worker is not worker
            or self._spawns[worker._worker_id]._termination_reason is not None
        ):
            return False
        self._retire_dependent_grant(worker)
        health = self._spawns[worker._worker_id]._health
        if health is not None:
            self._spawns[worker._worker_id]._health = replace(
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
        if not self._mark_current_worker_unready(worker, reason, watchdog_failure):
            return False
        evidence = (
            self._conditional_park_evidence(worker) if reason == 'lane-phase-overdue' else None
        )
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        if evidence is not None and state._record_park_evidence(evidence):
            self._park_evidence_by_group[(evidence._site, evidence._group)] = evidence
            self._spawns[worker._worker_id]._attributed_park_failure = True
        self._spawns[worker._worker_id]._termination_reason = reason
        worker._kill()
        return True

    def _expire_worker_lifecycle(self, lifecycle: _WorkerLifecycle) -> bool:
        """Fence startup if a receive or authentication consumed its remaining time."""
        now = self._clock()
        if now < lifecycle._deadline_at:
            return False
        self._contain_current_worker(lifecycle._worker, 'worker-startup-timeout')
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
        self._spawns[worker._worker_id]._termination_reason = reason
        worker._kill()

    @staticmethod
    def _operator_response_remaining(worker: _WorkerHandle, deadline: float) -> float:
        """Require the original response deadline at every batch boundary."""
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            error = TimeoutError('operator command response timed out')
            raise _WorkerControlError(worker, error, sending=False) from None
        return remaining

    def _receive_operator_response(
        self,
        worker: _WorkerHandle,
        deadline: float,
    ) -> ControlMessage:
        """Receive one bounded operator response or identify its control failure."""
        remaining = self._operator_response_remaining(worker, deadline)
        try:
            with self._normalize_worker_control_errors(worker):
                message = worker._receive(timeout=remaining)
        except TimeoutError as error:
            raise _WorkerControlError(worker, error, sending=False) from error
        self._advance_progress()
        return message

    def _release_source_ownership(self, worker: _WorkerHandle) -> None:
        """Release one exited source owner and stop its live dependents."""
        if not worker._spec.sources:
            return
        for key, owner in tuple(self._source_workers.items()):
            if owner is worker:
                self._source_workers.pop(key)
                self._source_endpoints.pop(key, None)
        for dependent_id, spawn in tuple(self._spawns.items()):
            dependent = spawn._worker
            if (
                dependent._spec.site == worker._spec.site
                and not dependent._spec.sources
                and self._spawns[dependent_id]._termination_reason is None
            ):
                self._retire_dependent_grant(dependent)
                self._spawns[dependent_id]._termination_reason = 'source-owner-unavailable'
                dependent._kill()

    def _runnable_worker_spec(self, spec: WorkerSpec) -> WorkerSpec:
        """Exclude pending dispositions from every worker's lane inventory."""
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        assignments = state._configured_group_assignments()
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
            delay = breaker._record_failure(reason)
        if delay is not None:
            self._restart_deadlines[worker_id] = (
                now + delay
                if breaker._restart_deadline_at is None
                else breaker._restart_deadline_at
            )
        state = self._state
        if state is None:
            raise RuntimeError('supervisor state is not open')
        state._persist_worker_breaker(breaker)

    def _reap_workers(self, now: float) -> bool:
        """Nonblockingly reap exits and assign independent restart deadlines."""
        changed = False
        for worker_id, spawn in tuple(self._spawns.items()):
            worker = spawn._worker
            status = worker._wait(timeout=0.0)
            self._advance_progress()
            if status is None:
                continue
            reason = self._spawns[worker_id]._termination_reason or f'worker-exit-{status}'
            attributed = self._spawns[worker_id]._attributed_park_failure
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
        status = worker._shutdown(reason)
        self._advance_progress()
        if status is None:
            self._halt_reason_value = f'worker child remained unreaped: {worker._worker_id}'
            self._progress.set_halted()
            raise RuntimeError(self._halt_reason_value)
        return status

    def _shutdown_workers(self, reason: str) -> None:
        """Stop every owned child in reverse startup order."""
        first_error: Exception | None = None
        first_halt_reason: str | None = None
        for spawn in reversed(tuple(self._spawns.values())):
            worker = spawn._worker
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
        """Stop every owned child and release the instance lock."""
        self._shutting_down = True
        self._progress.set_shutting_down()
        self._advance_progress()
        first_error: Exception | None = None
        try:
            self._shutdown_workers('supervisor-close')
            self._advance_progress()
        except Exception as error:  # noqa: BLE001
            first_error = error
        self._adopting_sources.clear()
        self._breakers.clear()
        self._park_evidence_by_group.clear()
        self._pending_dependents.clear()
        self._reconciled = None
        self._restart_deadlines.clear()
        self._specs.clear()
        state = self._state
        if state is not None:
            try:
                state.close()
            except Exception as error:  # noqa: BLE001
                if first_error is None:
                    first_error = error
            finally:
                self._state = None
        self._next_checkpoint_at = None
        lock = self._lock
        if lock is not None:
            try:
                lock.close()
            except Exception as error:  # noqa: BLE001
                if first_error is None:
                    first_error = error
            finally:
                self._lock = None
        if first_error is not None:
            raise first_error

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
            worker._worker_id
            for spawn in self._spawns.values()
            for worker in (spawn._worker,)
            if worker._spec.site == site and source in worker._spec.sources
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
            worker._worker_id
            for spawn in self._spawns.values()
            for worker in (spawn._worker,)
            if worker._spec.site == site
            and not worker._spec.sources
            and any(
                group in dependent_groups
                for configured_spec in build_worker_specs(self._config.sites[site])
                if configured_spec.worker_id == worker._worker_id
                for group in configured_spec.groups
            )
        )
        return source_worker_ids, dependent_ids

    def _source_adoption_target(
        self,
        command: OperatorCommand,
    ) -> tuple[str, str, str]:
        """Validate and return one configured source-adoption target."""
        if command.command != 'adopt-source-stream' or set(command.arguments) != {
            'reason',
            'site',
            'source',
        }:
            raise ValueError('source adoption command is invalid')
        site = command.arguments['site']
        source = command.arguments['source']
        reason = command.arguments['reason']
        if not isinstance(site, str) or site not in self._config.sites:
            raise ValueError('source adoption site is not configured')
        if not isinstance(source, str) or source not in self._config.sites[site].sources:
            raise ValueError('source adoption source is not configured')
        if not isinstance(reason, str) or not reason:
            raise ValueError('source adoption reason must be nonempty text')
        return site, source, reason

    def _source_writes(self, site: str, source: str) -> WriteContext:
        """Own one bounded source-file writer without worker watch or stop state."""
        lock = SourceWriteLock(
            source_write_lock_path(self._config, site, source),
            timeout_secs=OPERATOR_WRITE_BUDGET_SECS,
            wait_phase_grace_secs=self._config.daemon.health_interval_secs,
        )
        return WriteContext(lock, busy_is_command_failure=True)

    def _open_lifecycle_group(
        self,
        metadata: _OrphanGroupMetadata,
        *,
        group: str,
        operator: bool = False,
    ) -> GroupState:
        """Open one configured or absent group with stored or freshly observed source facts."""
        site = self._config.sites[metadata._site]
        source_config = site.sources[metadata._source]
        configured = site.groups.get(group)
        site_server_url = site.server
        source_generation = metadata._source_generation
        configured_fingerprint = source_stream_fingerprint(site, source_config)
        verified_incarnation = metadata._verified_incarnation
        opener = GroupState.open_for_disposition if operator else GroupState.open
        return opener(
            source_database_path(self._config, metadata._site, metadata._source),
            group_lock_path(self._config, metadata._site, group),
            writes=(
                self._source_writes(metadata._site, metadata._source).for_operator()
                if operator
                else self._source_writes(metadata._site, metadata._source)
            ),
            **group_state_options(self._config.daemon, source_config, configured),
            site=metadata._site,
            site_server_url=site_server_url,
            source=metadata._source,
            group=group,
            group_generation=metadata._group_generation,
            ordering=metadata._ordering if configured is None else configured.ordering,
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
            generations[(site_name, group_name)] = metadata._group_generation
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
                    group_generation=metadata._group_generation,
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
                    source_generation=metadata._source_generation,
                )
            )
        return states

    def _run_direct_disposition(
        self, command: OperatorCommand, metadata: _OrphanGroupMetadata, request: DispositionRequest
    ) -> CommandResult:
        """Execute only while both source and group ownership locks are available."""
        path = source_database_path(self._config, metadata._site, metadata._source)
        try:
            source_lock = acquire_lock(source_lock_path(path))
        except AlreadyLockedError as error:
            return CommandResult(
                command=command.command,
                ok=False,
                message=str(error),
                request_id=command.request_id,
                result={'code': 'source-busy'},
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
                    result={'code': 'group-busy'},
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
        if breaker._is_open:
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
        if breaker._is_open:
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
            if breaker._is_open:
                self._pending_dependents.pop(worker_id)
                continue
            if (
                breaker._restart_deadline_at is not None
                and breaker._restart_deadline_at > self._clock()
            ):
                self._restart_deadlines[worker_id] = breaker._restart_deadline_at
                self._pending_dependents.pop(worker_id)
                continue
            if not self._dependent_sources_ready(spec):
                continue
            self._pending_dependents.pop(worker_id)
            self._spawn_dependent(site_name, spec)
            started = True
        return started

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
            daemon.health_interval_secs,
        )

    def _trusted_watchdog_interval(
        self,
        watchdog: WatchdogHealth,
        sources: Mapping[str, SourceHealth],
    ) -> float:
        """Derive one phase budget entirely from supervisor configuration."""
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
            return self._internal_watchdog_budget(watchdog.site, source_name)
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
        site = self._config.sites[worker._spec.site]
        if watchdog.phase == 'idle':
            if watchdog.source is not None or watchdog.group is not None:
                raise ValueError('idle operator watchdog has an active target')
        elif watchdog.phase == 'shutdown' and watchdog.source is None:
            if watchdog.group is not None:
                raise ValueError('targetless operator shutdown has a group target')
        elif watchdog.group is not None:
            if (
                watchdog.group not in worker._spec.groups
                or watchdog.source != site.groups[watchdog.group].source
            ):
                raise ValueError('operator watchdog target conflicts with assignment')
        elif watchdog.source not in worker._spec.sources:
            raise ValueError('operator watchdog target conflicts with assignment')

    def _add_watchdog_to_inventory(
        self,
        worker: _WorkerHandle,
        watchdog: WatchdogHealth,
        source_watchdogs: dict[str, WatchdogHealth],
        lane_watchdogs: dict[str, WatchdogHealth],
    ) -> bool:
        """Validate and add one current configured watchdog owner."""
        site = self._config.sites[worker._spec.site]
        if watchdog.site != worker._spec.site:
            raise ValueError('worker watchdog site conflicts with current assignment')
        if watchdog.kind == 'source':
            source = watchdog.source
            if (
                source is None
                or source not in worker._spec.sources
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
                or group not in worker._spec.groups
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
        site = self._config.sites[worker._spec.site]
        source_records = {source.source: source for source in message.sources}
        group_records = {group.group: group for group in message.groups}
        expected_groups = set(worker._spec.groups) | self._pending_worker_groups(worker)
        if len(source_records) != len(message.sources) or any(
            source.site != worker._spec.site or source.source not in worker._spec.sources
            for source in message.sources
        ):
            raise ValueError('worker source inventory conflicts with current assignment')
        if len(group_records) != len(message.groups) or any(
            group.site != worker._spec.site
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
            _source_records=source_records,
            _group_records=group_records,
            _source_watchdogs=source_watchdogs,
            _lane_watchdogs=lane_watchdogs,
            _operator_count=operator_count,
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
            state = _SupervisorState._open(
                state_dir,
                synchronous=self._config.daemon.sqlite_synchronous,
                interval_secs=self._config.daemon.checkpoint_interval_secs,
                clock=self._clock,
                checkpointer_factory=self._checkpointer_factory,
            )
            pending_evidence = state._pending_park_evidence()
        except BaseException as error:
            close_resources((state, lock), primary_error=error)
            raise
        self._lock = lock
        self._state = state
        self._park_evidence_by_group = {
            (evidence._site, evidence._group): evidence for evidence in pending_evidence
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
        worker_id = worker._worker_id
        health = message.worker
        if health.spawn_instance != worker._spawn_instance or any(
            item.worker_instance != worker._spawn_instance
            for item in (*message.watchdogs, *message.sources, *message.groups)
        ):
            return False
        if health.site != worker._spec.site or health.worker_id != worker_id:
            raise ValueError('worker health identity conflicts with current spawn')
        inventory = self._validated_health_inventory(worker, message)
        effective, invalid_reason = self._effective_watchdogs(
            message.watchdogs,
            inventory._source_records,
            now,
        )
        current_worker = replace(
            health,
            heartbeat_at=min(now, health.heartbeat_at),
            heartbeat_age=max(0.0, now - min(now, health.heartbeat_at)),
        )
        self._spawns[worker_id]._health = self._refresh_component_health(
            replace(message, worker=current_worker, watchdogs=effective),
            now,
        )
        inventory_reason = self._inventory_failure_reason(worker, inventory)
        complete = invalid_reason is None and inventory_reason is None
        if health.watchdog_failure is not None:
            invalid_reason = 'watchdog-sampling-failed'
        elif self._spawns[worker_id]._health_inventory_complete and not complete:
            invalid_reason = invalid_reason or inventory_reason
        if invalid_reason is not None or not health.monitor_alive:
            self._spawns[worker_id]._health_failure = invalid_reason or 'worker-heartbeat-overdue'
            self._spawns[worker_id]._health_deadline = now
        elif complete:
            self._spawns[worker_id]._health_inventory_complete = True
            self._spawns[worker_id]._health_failure = None
            self._spawns[worker_id]._health_deadline = (
                current_worker.heartbeat_at
                + self._config.daemon.health_interval_secs * _WORKER_STALE_INTERVALS
            )
        return True

    @staticmethod
    def _worker_control_failure_reason(failure: _WorkerControlError) -> str:
        """Return one bounded reason for an identified channel failure."""
        if failure._sending:
            if isinstance(failure._error, TimeoutError):
                return 'worker-control-send-timeout'
            return 'worker-control-send-failed'
        return 'worker-control-eof'

    def _contain_worker_control_failure(self, failure: _WorkerControlError) -> None:
        """Contain the exact current worker that authored a control failure."""
        self._fail_current_worker(
            failure._worker,
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
        reconciled = state._reconcile_groups(
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
                reconciled = state._reconcile_groups(
                    configured=configured,
                    generations=current_group_generations,
                )
        self._reconciled = reconciled

    def _worker_health_failure(
        self,
        worker_id: str,
        health: HealthUpdate | None,
        now: float,
    ) -> str | None:
        """Return the first current bounded worker containment reason."""
        reason = self._spawns[worker_id]._health_failure
        if reason is not None:
            return reason
        if health is not None:
            for watchdog in health.watchdogs:
                if self._phase_confirmed_overdue(health.worker.heartbeat_at, watchdog.deadline_at):
                    return self._watchdog_overdue_reason(watchdog)
        if now >= self._spawns[worker_id]._health_deadline:
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
            self._spawns[worker._worker_id]._health_failure
            if self._message_batches is not None
            else self._worker_health_failure(
                worker._worker_id,
                self._spawns[worker._worker_id]._health,
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
            and (spawn := self._spawns.get(worker._worker_id)) is not None
            and spawn._worker is worker
            and self._source_workers.get((worker._spec.site, message.source)) is worker
        ):
            self._stale_grant_readies.remove(message)
            return False
        if isinstance(message, GroupRecoveryReady):
            lifecycle = self._spawns[worker._worker_id]._lifecycle
            if lifecycle is None or lifecycle._worker is not worker:
                raise ValueError('group recovery ready has no current channel lifecycle')
            self._accept_group_recovery_ready(lifecycle, message)
            self._complete_worker_lifecycle(lifecycle)
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
        lifecycle._grant_index += 1
        lifecycle._grant_sent = False
        return lifecycle._grant_index >= len(lifecycle._assignment.event_grants)

    def _accept_operator_batch(
        self,
        worker: _WorkerHandle,
        command: OperatorCommand,
        messages: Iterator[ControlMessage],
        deadline: float,
    ) -> CommandResult | None:
        """Retain the correlated result until its finite health batch is judged."""
        if self._message_batches is not None:
            raise RuntimeError('worker message acquisition is not reentrant')
        result = None
        self._message_batches = {}
        try:
            for message in self._receive_operator_messages(worker, messages):
                self._operator_response_remaining(worker, deadline)
                retired = self._accept_worker_channel_message(worker, message)
                if retired is True:
                    raise ValueError('operator worker health failed')
                if retired is None:
                    if isinstance(message, CommandResult):
                        if message.request_id == command.request_id:
                            if message.command != command.command:
                                raise ValueError('operator command result identity conflicts')
                            result = message
                    else:
                        raise TypeError(
                            f'unexpected operator response from worker {worker._worker_id}: '
                            f'{type(message).__name__}'
                        )
                self._advance_progress()
        finally:
            self._message_batches = None
        self._operator_response_remaining(worker, deadline)
        now = self._clock()
        reason = self._worker_health_failure(
            worker._worker_id, self._spawns[worker._worker_id]._health, now
        )
        if reason is not None:
            self._contain_current_worker(worker, reason)
            raise ValueError('operator worker health failed')
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
            f'unexpected monitor message from worker {worker._worker_id}: {type(message).__name__}'
        )

    def _advance_dependent_grant(
        self,
        lifecycle: _WorkerLifecycle,
    ) -> bool:
        """Advance one dependent grant and report whether all grants are ready."""
        grants = lifecycle._assignment.event_grants
        if lifecycle._grant_index >= len(grants):
            return True
        grant = grants[lifecycle._grant_index]
        worker = lifecycle._worker
        source_worker = self._source_workers[(worker._spec.site, grant.source)]
        if self._spawns[source_worker._worker_id]._termination_reason is not None:
            return False
        owner = self._active_dependent_grant_owner(source_worker)
        if owner is not None and owner is not lifecycle:
            return False
        if not lifecycle._grant_sent:
            with self._normalize_worker_control_errors(source_worker, sending=True):
                source_worker._send(grant)
            lifecycle._grant_sent = True
        try:
            with self._normalize_worker_control_errors(source_worker):
                message = self._receive_worker_message(source_worker)
        except TimeoutError:
            return False
        expired = self._expire_worker_lifecycle(lifecycle)
        ready = self._accept_dependent_grant_message(
            lifecycle,
            grant,
            source_worker,
            message,
        )
        return ready and not expired

    def _advance_ready_lifecycle(
        self,
        lifecycle: _WorkerLifecycle,
    ) -> bool:
        """Ingest health or recovery while a ready spawn remains fenced."""
        worker = lifecycle._worker
        try:
            with self._normalize_worker_control_errors(worker):
                message = self._receive_worker_message(worker)
        except TimeoutError:
            return False
        if self._expire_worker_lifecycle(lifecycle):
            return True
        if self._accept_worker_channel_message(worker, message) is None:
            raise TypeError('worker lifecycle message conflicts')
        return True

    def _advance_dependent_lifecycle(
        self,
        lifecycle: _WorkerLifecycle,
    ) -> bool:
        """Advance one dependent through auth, grants, and assignment."""
        if lifecycle._ready:
            return self._advance_ready_lifecycle(lifecycle)
        worker = lifecycle._worker
        if not lifecycle._authenticated:
            with self._normalize_worker_control_errors(worker):
                authenticated = worker._authenticate(timeout=0.0)
            if not authenticated:
                return False
            lifecycle._authenticated = True
        if self._expire_worker_lifecycle(lifecycle):
            return True
        if not self._advance_dependent_grant(lifecycle):
            return True
        with self._normalize_worker_control_errors(worker, sending=True):
            worker._assign(lifecycle._assignment)
        lifecycle._assigned = True
        lifecycle._ready = True
        self._register_worker(worker)
        self._complete_worker_lifecycle(lifecycle)
        return True

    def _advance_source_message(
        self,
        lifecycle: _WorkerLifecycle,
        message: ControlMessage,
    ) -> bool:
        """Ingest one source startup message and report endpoint readiness."""
        worker = lifecycle._worker
        if self._accept_worker_channel_message(worker, message) is not None:
            return False
        if not isinstance(message, SourceEndpointReady):
            raise TypeError('worker source endpoint message conflicts')
        expected_generations = dict(lifecycle._assignment.source_generations)
        if (
            message.source in lifecycle._endpoint_messages
            or expected_generations.get(message.source) != message.source_generation
        ):
            raise ValueError('worker source endpoint identity conflicts')
        lifecycle._endpoint_messages[message.source] = message
        return set(lifecycle._endpoint_messages) == set(expected_generations)

    def _advance_source_lifecycle(
        self,
        lifecycle: _WorkerLifecycle,
    ) -> bool:
        """Advance one source owner through auth, assignment, and endpoints."""
        if lifecycle._ready:
            return self._advance_ready_lifecycle(lifecycle)
        worker = lifecycle._worker
        if not lifecycle._authenticated:
            with self._normalize_worker_control_errors(worker):
                authenticated = worker._authenticate(timeout=0.0)
            if not authenticated:
                return False
            lifecycle._authenticated = True
        if self._expire_worker_lifecycle(lifecycle):
            return True
        if not lifecycle._assigned:
            with self._normalize_worker_control_errors(worker, sending=True):
                worker._assign(lifecycle._assignment)
            lifecycle._assigned = True
        try:
            with self._normalize_worker_control_errors(worker):
                message = self._receive_worker_message(worker)
        except TimeoutError:
            return True
        if self._expire_worker_lifecycle(lifecycle) or not self._advance_source_message(
            lifecycle, message
        ):
            return True
        lifecycle._ready = True
        self._register_worker(worker)
        for source, endpoint in lifecycle._endpoint_messages.items():
            key = (worker._spec.site, source)
            self._source_endpoints[key] = endpoint
            self._source_workers[key] = worker
        self._start_ready_dependents(worker._spec.site)
        self._complete_worker_lifecycle(lifecycle)
        return True

    def _advance_lifecycles(self, now: float) -> bool:
        """Give every worker lifecycle one nonblocking state-machine step."""
        changed = False
        grant_workers = self._grant_worker_ids()
        for worker_id, lifecycle in tuple(
            (worker_id, spawn._lifecycle)
            for worker_id, spawn in self._spawns.items()
            if spawn._lifecycle is not None
        ):
            if (
                self._spawns[worker_id]._termination_reason is not None
                or self._spawns[worker_id]._lifecycle is not lifecycle
            ):
                continue
            if now >= lifecycle._deadline_at:
                changed = (
                    self._contain_current_worker(lifecycle._worker, 'worker-startup-timeout')
                    or changed
                )
                continue
            if worker_id in grant_workers:
                continue
            try:
                if lifecycle._assignment.source_generations:
                    progressed = self._advance_source_lifecycle(lifecycle)
                else:
                    progressed = self._advance_dependent_lifecycle(lifecycle)
            except _WorkerControlError as failure:
                self._contain_worker_control_failure(failure)
                progressed = True
            except (EOFError, TypeError, ValueError) as error:
                progressed = self._contain_current_worker(
                    lifecycle._worker, f'worker-startup-{type(error).__name__}'
                )
            changed = progressed or changed
            self._advance_progress()
        return changed

    def _ingest_worker_ipc(self) -> bool:
        """Drain spawn-matched health, stale-grant, and late `CommandResult` traffic."""
        ingested = False
        grant_workers = self._grant_worker_ids()
        for worker_id, spawn in tuple(self._spawns.items()):
            worker = spawn._worker
            if (
                self._spawns[worker_id]._lifecycle is not None
                or self._spawns[worker_id]._termination_reason is not None
                or worker_id in grant_workers
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
                retired = self._accept_worker_monitor_message(worker, message)
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
            worker = spawn._worker
            if self._spawns[worker_id]._termination_reason is not None:
                continue
            health = self._spawns[worker_id]._health
            if health is not None:
                health = self._refresh_component_health(health, now)
                self._spawns[worker_id]._health = health
            reason = self._worker_health_failure(worker_id, health, now)
            if reason is None:
                lifecycle = self._spawns[worker_id]._lifecycle
                pending_recovery = lifecycle is not None and bool(lifecycle._pending_recovery)
                if (
                    health is not None
                    and self._spawns[worker_id]._health_inventory_complete
                    and not pending_recovery
                    and self._breakers[worker_id]._record_healthy()
                ):
                    state = self._state
                    if state is None:
                        raise RuntimeError('supervisor state is not open')
                    state._persist_worker_breaker(self._breakers[worker_id])
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
                changed = self._advance_lifecycles(self._clock()) or changed
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
        old = self._spawns[worker_id]._worker
        self._shutdown_worker(old, 'worker-restart')
        self._discard_worker_lifecycle(old)
        if spec.sources:
            self._release_source_ownership(old)
            self._spawn_source(spec)
        else:
            self._spawn_dependent(spec.site, spec)
        self._advance_worker_messages()

    def _restart_source_owners(
        self,
        site: str,
        source: str,
        groups: Collection[str],
        result: CommandResult,
    ) -> CommandResult:
        """Rebuild source inventory and dependent lanes after durable recovery."""
        source_worker_ids, dependent_ids = self._source_adoption_restart_ids(
            site,
            source,
            groups,
        )
        for worker_id in dependent_ids:
            worker = self._spawns[worker_id]._worker
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
                spawn(worker_id)
                replacements[worker_id] = self._spawns[worker_id]._worker
                self._advance_progress()
            while replacements:
                self._advance_worker_messages()
                self._advance_progress()
                for worker_id, worker in tuple(replacements.items()):
                    reason = self._spawns[worker_id]._termination_reason
                    if reason is not None:
                        return replace(
                            result,
                            ok=False,
                            message=f'worker restart failed: {worker_id} ({reason})',
                            result={},
                        )
                    lifecycle = self._spawns[worker_id]._lifecycle
                    ready = (lifecycle is None or lifecycle._ready) and (
                        not worker._spec.sources
                        or all(
                            self._source_workers.get((worker._spec.site, source_name)) is worker
                            for source_name in worker._spec.sources
                        )
                    )
                    if ready:
                        replacements.pop(worker_id)
                    self._advance_progress()
                deadline_at = min(
                    (
                        lifecycle._deadline_at
                        for worker_id in replacements
                        if (lifecycle := self._spawns[worker_id]._lifecycle) is not None
                    ),
                    default=self._clock(),
                )
                remaining = max(0.0, deadline_at - self._clock())
                self._advance_progress()
                time.sleep(
                    min(
                        remaining,
                        self._config.daemon.supervisor_poll_interval_secs,
                    )
                )
        return result

    def _complete_disposition_inventory(
        self,
        command: OperatorCommand,
        reservation: PendingDispositionSourceGroupAssignment,
        result: CommandResult,
    ) -> CommandResult:
        """Complete only the matching reservation, preserving any newer pending intent."""
        state = self._state
        if state is None:
            raise RuntimeError('group disposition authority is unavailable')
        site = cast('str', command.arguments['site'])
        group = reservation.group
        record = state._group_inventory_record(site=site, group=group)
        if (
            record is None
            or record._generation != reservation.reserved_generation
            or record._orphan_generation != reservation.orphan_generation
        ):
            return result
        if not record._configured:
            state._complete_orphan_disposition(
                site=site,
                group=group,
                orphan_generation=reservation.orphan_generation,
                reserved_generation=reservation.reserved_generation,
            )
            return result
        active = state._complete_group_disposition(
            site=site,
            group=group,
            orphan_generation=reservation.orphan_generation,
            reserved_generation=reservation.reserved_generation,
        )
        pending = self._reconciled is not None and isinstance(
            self._reconciled.get((site, group)), PendingDispositionSourceGroupAssignment
        )
        if self._reconciled is not None:
            self._reconciled[(site, group)] = active
        if pending:
            return self._restart_source_owners(site, active.source, (group,), result)
        return result

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
        changed = self._advance_worker_messages()
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
                state._checkpoint()
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

    def dispatch_operator_command(
        self,
        command: OperatorCommand,
        *,
        timeout: float,
    ) -> CommandResult:
        """Send one request to its owning worker and correlate the result.

        A response wait timeout or normalized channel failure contains the
        owning worker. Neither failure rolls back an already dispatched
        mutation or establishes its outcome.

        Args:
            command: Trusted body-free operator command.
            timeout: Positive maximum response wait in seconds after dispatch.

        Returns:
            Exact request-correlated worker result.

        Raises:
            TimeoutError: If the owning worker does not answer in time.
            ValueError: If the timeout is not positive, the target is invalid,
                or the response conflicts.
        """
        if timeout <= 0:
            raise ValueError('operator command timeout must be positive')
        worker = self._operator_command_owner(command)
        transport_command = self._next_operator_transport_command(command)
        try:
            with self._normalize_worker_control_errors(worker, sending=True):
                worker._send(transport_command)
            self._advance_progress()
            deadline = time.monotonic() + timeout
            while True:
                message = self._receive_operator_response(worker, deadline)
                with self._normalize_worker_control_errors(worker):
                    messages = worker._receive_available()
                result = self._accept_operator_batch(
                    worker, transport_command, chain((message,), messages), deadline
                )
                if result is not None:
                    return replace(result, request_id=command.request_id)
        except _WorkerControlError as failure:
            self._contain_worker_control_failure(failure)
            raise failure._error from failure

    def _dispatch_source_adoption(
        self,
        command: OperatorCommand,
        *,
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
            return result
        if (
            result.result.get('initialized') is not True
            or result.result.get('status') != 'running'
        ):
            raise RuntimeError('source adoption returned before replacement initialization')
        site = cast('str', command.arguments['site'])
        source = cast('str', command.arguments['source'])
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
        for group in groups:
            assignment = state._configured_group_assignments().get((site, group))
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
                    raise ValueError(f'source adoption could not park group {group!r}')
                observed_generation = park.result.get('group_generation')
                if type(observed_generation) is not int:
                    raise ValueError('parked group generation is unavailable')
                assignment = state._reserve_group_disposition(
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
        self, command: OperatorCommand, record: _InventoryRecord, timeout: float
    ) -> tuple[CommandResult | None, bool]:
        """Validate a running lane or return an exact receipt before reserving new work."""
        if not record._configured or record._lifecycle != 'active':
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
                    'orphan_generation': record._generation,
                    'reserved_generation': record._generation,
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
                        'orphan_generation': metadata._group_generation,
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
                'source': metadata._source,
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
        record = state._group_inventory_record(site=site, group=group)
        if record is None:
            raise ValueError('group disposition target is not configured')
        probe, lane_admitted = self._probe_running_disposition(command, record, timeout)
        if probe is not None:
            return probe
        metadata = self._orphan_group_metadata(site=site, group=group)
        if (
            metadata._site != site
            or metadata._source != record._source
            or metadata._group_generation not in {record._generation, record._orphan_generation}
        ):
            raise RuntimeError('group disposition inventory conflicts with group state')
        recorded_generations, revision, admissible = self._disposition_observation(
            site, group, disposition_id
        )
        if recorded_generations is None:
            if record._configured and not lane_admitted and not admissible:
                raise InvalidTransitionError('position changes require a parked or orphaned group')
            reservation = state._reserve_group_disposition(
                site=site, group=group, observed_generation=metadata._group_generation
            )
        else:
            reservation = PendingDispositionSourceGroupAssignment(
                source=metadata._source,
                group=group,
                orphan_generation=recorded_generations[0],
                reserved_generation=recorded_generations[1],
            )
        request = disposition.request(group, reservation.reserved_generation, revision)
        result = self._route_disposition(command, metadata, request, timeout)
        if not result.ok:
            return result
        if result.result.get('group_generation') != reservation.reserved_generation:
            raise RuntimeError('group disposition result generation conflicts')
        if recorded_generations is not None:
            result = replace(result, message='recorded')
        return self._complete_disposition_inventory(command, reservation, result)

    def adopt_source_stream_command(
        self,
        command: OperatorCommand,
        *,
        timeout: float,
    ) -> CommandResult:
        """Park every configured group before dispatching source adoption.

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
            TimeoutError: An owning worker misses its response deadline.
            RuntimeError: Required disposition authority is unavailable or the
                adoption result does not confirm initialized running state.
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
                    reservation=(adoption_id, command_fingerprint),
                    groups=groups,
                    timeout=timeout,
                )
            finally:
                self._adopting_sources.discard(identity)
        except SourceDatabaseBusyError as error:
            return CommandResult(
                command=command.command,
                ok=False,
                message=str(error),
                request_id=command.request_id,
                result={'code': 'source-database-busy'},
            )

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
            TimeoutError: An owning worker misses its response deadline.
            InvalidTransitionError: The group is active and unparked, or direct
                disposition execution rejects its state, authority, or
                requested position.
            RuntimeError: Disposition authority or durable state conflicts.
        """
        try:
            return self._dispatch_group_disposition(command, timeout)
        except SourceDatabaseBusyError as error:
            return CommandResult(
                command=command.command,
                ok=False,
                message=str(error),
                request_id=command.request_id,
                result={'code': 'source-database-busy'},
            )

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

    def reconcile_durable_state(self) -> None:
        """Discover locked durable authorities and reconcile before spawning.

        This startup boundary opens every existing configured or retained
        source and group database under its owner lock, performs disposition
        recovery, and releases those locks before worker construction.

        Raises:
            RuntimeError: The supervisor is not open, workers have already been
                spawned, or retained source/group authorities cannot be
                reconciled.
        """
        if self._spawns:
            raise RuntimeError('startup reconciliation cannot run after worker spawn')
        state = self._state
        if state is None:
            raise RuntimeError('supervisor is not open')
        configured = {
            (site.name, group.name): group.source
            for site in self._config.sites.values()
            for group in site.groups.values()
        }
        records = state._inventory_records()
        group_identities = set(configured) | set(records)
        source_identities = {
            (site.name, source.name)
            for site in self._config.sites.values()
            for source in site.sources.values()
        } | {(site, record._source) for (site, _group), record in records.items()}
        with ExitStack() as stack:
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
                configured, the reason exceeds 4096 UTF-8 bytes, or the breaker
                is not open at the expected revision.
            RuntimeError: The supervisor or breaker authority is unavailable.
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
        spec = self._specs.get(worker_id)
        if spec is None or spec.site != site:
            raise ValueError('worker breaker reset target is not configured')
        breaker = self._breakers.get(worker_id)
        state = self._state
        if breaker is None or state is None:
            raise RuntimeError('worker breaker reset authority is unavailable')
        transition = state._reset_worker_breaker(
            breaker,
            expected_revision=breaker._revision,
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
            'revision': breaker._revision,
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
            DatabaseError: Checkpointing fails; the supervisor is halted and
                worker shutdown is attempted before the failure propagates.
        """
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
                wait(timeout)
        finally:
            set_foreground_progress(None)

    def start(self) -> None:
        """Begin source-first staged startup without waiting for readiness.

        Raises:
            RuntimeError: The supervisor is not open, durable state has not
                been reconciled, or workers are already started.
        """
        if self._lock is None or self._state is None:
            raise RuntimeError('supervisor is not open')
        if self._reconciled is None:
            raise RuntimeError('supervisor must reconcile before spawning workers')
        if self._spawns or self._pending_dependents:
            raise RuntimeError('supervisor workers are already started')
        try:
            for site in self._config.sites.values():
                specs = build_worker_specs(site)
                self._specs.update((spec.worker_id, spec) for spec in specs)
                source_spec = specs[0]
                self._pending_dependents.update((spec.worker_id, spec) for spec in specs[1:])
                breaker = self._ensure_worker_breaker(source_spec)
                if breaker._is_open:
                    continue
                if (
                    breaker._restart_deadline_at is not None
                    and breaker._restart_deadline_at > self._clock()
                ):
                    self._restart_deadlines[source_spec.worker_id] = breaker._restart_deadline_at
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
        """
        if set(arguments).difference({'group', 'site'}):
            raise ValueError('status arguments are invalid')
        site_filter = arguments.get('site')
        group_filter = arguments.get('group')
        if site_filter is not None and (
            not isinstance(site_filter, str) or site_filter not in self._config.sites
        ):
            raise ValueError('status site is not configured')
        if group_filter is not None and not isinstance(group_filter, str):
            raise ValueError('status group is invalid')
        if group_filter is not None:
            matching_sites = tuple(
                site.name
                for site in self._config.sites.values()
                if group_filter in site.groups
                and (site_filter is None or site.name == site_filter)
            )
            if not matching_sites:
                raise ValueError('status group is not configured')

        now = self._clock()
        updates = tuple(
            self._refresh_component_health(update, now)
            for spawn in self._spawns.values()
            if (update := spawn._health) is not None
        )
        for update in updates:
            self._spawns[update.worker.worker_id]._health = update
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
            transition for breaker in self._breakers.values() for transition in breaker._history
        )
        state = self._state
        if state is None:
            raise RuntimeError('supervisor status authority is unavailable')
        database_sizes = (
            ('supervisor.db', state._database_size_bytes()),
            *(
                (
                    f'sites/{source.site}/sources/{source.source}.db',
                    source.database_size_bytes,
                )
                for source in sources
            ),
        )
        snapshot = aggregate_status(
            sources=sources,
            groups=groups,
            workers=workers,
            now=now,
            stale_after_secs=(self._config.daemon.health_interval_secs * _WORKER_STALE_INTERVALS),
            audit_count=(
                state._audit_count()
                + sum(source.audit_count for source in sources)
                + sum(group.audit_count for group in groups)
            ),
            database_sizes=database_sizes,
            breaker_history=breaker_history,
            breaker_transition_count=sum(
                breaker._transition_count for breaker in self._breakers.values()
            ),
            worker_breaker_open=any(breaker._is_open for breaker in self._breakers.values()),
        )
        if self._halt_reason_value is not None:
            self._progress.set_halted()
        probe = self._progress.result(now=now)
        payload = dict(snapshot.to_mapping())
        payload['liveness'] = probe.liveness
        payload['readiness'] = probe.readiness
        conditions = probe.conditions if probe.generated_at is not None else ('stale',)
        payload['conditions'] = conditions
        payload['primary_state'] = primary_state_for_conditions(conditions)
        return _budget_status_payload(
            payload,
            group_filter=group_filter,
            site_filter=site_filter,
        )


@dataclass
class _WorkerInstance:
    """Own one spawned child through authentication, assignment, readiness, and shutdown."""

    _spec: WorkerSpec
    _spawn_instance: str
    _process: ChildProcess
    _control: WorkerControl
    _shutdown_deadline_secs: float
    _wait_slice_secs: float = 0.5
    _foreground_progress: Callable[[], None] | None = None
    _assigned_worker: WorkerAssignment | None = None
    _authenticated: bool = False
    _closed: bool = False

    def _advance_foreground_progress(self) -> None:
        """Record one completed foreground-owned bounded substep."""
        if self._foreground_progress is not None:
            self._foreground_progress()

    def _assign(self, assignment: WorkerAssignment) -> None:
        """Deliver the exact immutable assignment once."""
        if self._assigned_worker is not None:
            raise ValueError('worker assignment is already delivered')
        if not self._authenticated:
            raise RuntimeError('worker must authenticate before assignment')
        if (
            assignment.worker_id != self._spec.worker_id
            or assignment.spawn_instance != self._spawn_instance
        ):
            raise ValueError('worker assignment identity conflicts with spawn')
        self._control.send(assignment)
        self._assigned_worker = assignment

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

    def _close(self) -> None:
        """Close control so the worker begins bounded graceful shutdown."""
        if not self._closed:
            self._control.close()
            self._closed = True
            self._advance_foreground_progress()

    def _kill(self) -> None:
        """Force termination when the child remains alive."""
        terminate_child(self._process, force=True)
        self._advance_foreground_progress()

    @property
    def _pid(self) -> int:
        """Return the operating-system child pid."""
        return self._process.pid

    def _authenticate(self, *, timeout: float = 0.0) -> bool:
        """Advance exact first-message authentication within one bound."""
        if self._authenticated:
            return True
        try:
            message = self._control.receive(timeout=timeout)
        except TimeoutError:
            return False
        self._authenticate_hello(
            message,
            spec=self._spec,
            spawn_instance=self._spawn_instance,
            pid=self._pid,
        )
        self._authenticated = True
        return True

    def _receive(self, *, timeout: float) -> ControlMessage:
        """Receive one typed child control message."""
        return self._control.receive(timeout=timeout)

    def _receive_available(self) -> Iterator[ControlMessage]:
        """Freeze the sole reader's current child control message batch."""
        return self._control.receive_available()

    def _send(self, message: ControlMessage) -> None:
        """Send one typed post-assignment control message."""
        if self._assigned_worker is None:
            raise RuntimeError('worker must receive its assignment first')
        self._control.send(message)

    def _shutdown(self, reason: str) -> int | None:
        """Request clean stop, then kill after the configured deadline."""
        result = reap_child(self._process)
        self._advance_foreground_progress()
        if result is not None:
            self._close()
            return result
        with suppress(EOFError, OSError):
            self._control.send(StopCommand(reason=reason))
            self._advance_foreground_progress()
        result = _reap_child_until(
            self._process,
            deadline_secs=self._shutdown_deadline_secs,
            wait_slice_secs=self._wait_slice_secs,
            foreground_progress=self._foreground_progress,
        )
        if result is not None:
            self._close()
            return result
        terminate_child(self._process, force=True)
        self._advance_foreground_progress()
        result = _reap_child_until(
            self._process,
            deadline_secs=self._shutdown_deadline_secs,
            wait_slice_secs=self._wait_slice_secs,
            foreground_progress=self._foreground_progress,
        )
        self._close()
        return result

    @classmethod
    def _spawn(cls, **options: Unpack[_WorkerSpawnOptions]) -> Self:
        """Spawn one unauthenticated, unassigned worker and own its control channel."""
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
                send_deadline_secs=options['send_deadline_secs'],
            )
            instance = cls(
                _spec=spec,
                _spawn_instance=spawn_instance,
                _process=process,
                _control=control,
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

    def _wait(self, *, timeout: float = 0.0) -> int | None:
        """Return the child status within one bounded wait."""
        return reap_child(self._process, timeout=timeout)

    @property
    def _worker_id(self) -> str:
        """Return the packed worker identity."""
        return self._spec.worker_id


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
            _configured=bool(row[6]),
            _generation=int(row[3]),
            _lifecycle=lifecycle,
            _orphan_generation=None if row[4] is None else int(row[4]),
            _source=str(row[2]),
            _start_mode=start_mode,
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
        None if request._previous is None else request._previous._generation,
        request._group_generation,
    )
    if not observed:
        return (
            _allocate_generation(
                connection,
                site=request._site,
                group=request._group,
                observed_generations=(),
            ),
            'initialize',
        )
    generation = max(observed)
    _record_generation_high_water(
        connection,
        site=request._site,
        group=request._group,
        generation=generation,
    )
    if request._group_generation is not None:
        if request._group_generation != generation:
            raise RuntimeError('greatest reconciled generation lacks matching group state')
        return generation, 'resume'
    if (
        request._previous is not None
        and request._previous._generation == generation
        and request._previous._start_mode == 'initialize'
    ):
        return generation, 'initialize'
    raise RuntimeError('resume group state is missing')


def _reconcile_parked_inventory(
    connection: sqlite3.Connection,
    request: _InventoryReconciliation,
) -> PendingDispositionSourceGroupAssignment:
    """Validate and project one still-pending re-added inventory fence."""
    previous = request._previous
    if previous is None:
        raise RuntimeError('parked reconciliation requires prior inventory')
    orphan_generation = previous._parked_orphan_generation()
    observed = _observed_generations(
        previous._generation,
        request._group_generation,
    )
    greatest = max(observed)
    _record_generation_high_water(
        connection,
        site=request._site,
        group=request._group,
        generation=greatest,
    )
    if greatest > previous._generation:
        raise RuntimeError('durable generation exceeds reserved re-add generation')
    if request._group_generation not in (
        None,
        orphan_generation,
        previous._generation,
    ):
        raise RuntimeError('group generation conflicts with re-add inventory')
    return PendingDispositionSourceGroupAssignment(
        source=request._source,
        group=request._group,
        orphan_generation=orphan_generation,
        reserved_generation=previous._generation,
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
            request._site,
            request._group,
            request._source,
            generation,
            start_mode,
        ),
    )


def _reconcile_inventory_group(
    connection: sqlite3.Connection,
    request: _InventoryReconciliation,
) -> SourceGroupAssignment:
    """Reconcile one configured identity to runnable or parked inventory."""
    if not request._site or not request._source or not request._group:
        raise ValueError('configured group identity must not be empty')
    previous = request._previous
    if previous is not None and previous._source != request._source:
        raise RuntimeError('configured group source changed without adoption')
    if previous is not None and (not previous._configured or previous._lifecycle == 'orphaned'):
        orphan_generation = previous._generation
        generation = _allocate_generation(
            connection,
            site=request._site,
            group=request._group,
            observed_generations=_observed_generations(
                previous._generation,
                request._group_generation,
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
            (generation, orphan_generation, request._site, request._group),
        )
        return PendingDispositionSourceGroupAssignment(
            source=request._source,
            group=request._group,
            orphan_generation=orphan_generation,
            reserved_generation=generation,
        )
    if previous is not None and previous._lifecycle == 'readded-parked':
        return _reconcile_parked_inventory(connection, request)
    generation, start_mode = _reconcile_active_generation(connection, request)
    _store_active_inventory(
        connection,
        request,
        generation=generation,
        start_mode=start_mode,
    )
    return ActiveSourceGroupAssignment(
        source=request._source,
        group=request._group,
        group_generation=generation,
        start_mode=start_mode,
    )
