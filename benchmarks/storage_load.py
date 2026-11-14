"""Measure revision 5 storage through production source, lane, and checkpoint paths.

Run `storage_load.py run --cell all` with the work/result directories and
device metadata. Duration scaling is diagnostic only; qualification uses the
default durations and populations. Raw observations accompany each verdict.
"""

from __future__ import annotations

import argparse
import cProfile
import json
import logging
import math
import mmap
import multiprocessing
import os
import queue
import signal
import sqlite3
import struct
import sys
import threading
import time
import traceback
from collections import Counter
from contextlib import ExitStack, closing, contextmanager
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from itertools import pairwise
from multiprocessing.process import BaseProcess
from pathlib import Path
from statistics import median
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NotRequired,
    TypedDict,
    Unpack,
    cast,
)

from sg_event_daemon import _checkpoint
from sg_event_daemon._checkpoint import CheckpointAttempt, Checkpointer
from sg_event_daemon._checkpoint_host import CheckpointHost
from sg_event_daemon._config import (
    CredentialConfig,
    GroupConfig,
    SecretValue,
    SiteConfig,
    SourceConfig,
)
from sg_event_daemon._events import EVENT_FIELDS, Event
from sg_event_daemon._group_state import GroupState
from sg_event_daemon._group_types import CallbackDoneStep
from sg_event_daemon._health import ConditionFlags
from sg_event_daemon._lane import Delivery, GroupLane
from sg_event_daemon._platform import ensure_secure_state_dir
from sg_event_daemon._plugins import CallbackIdentity, CallbackSpec, EventContext, PluginSpec
from sg_event_daemon._providers import Provider, ProviderCapabilities
from sg_event_daemon._source_schema import SOURCE_SCHEMA, source_identity
from sg_event_daemon._source_state import SourceState
from sg_event_daemon._sqlite import WAL_IN_USE_LIMIT_BYTES, wal_file_bytes
from sg_event_daemon._watchdog import ComponentWatchdog
from sg_event_daemon._write_lock import (
    SourceWriteLock,
    WriteContext,
    WriteLockStoppedError,
    WriteLockTimeoutError,
    WritePhase,
    WriteTiming,
)
from tests._storage_support import source_database
from tests._worker_processes import (
    ChildPatches,
    SupervisorProbe,
    Topology,
    WorkerSpawn,
    running_supervisor,
    write_daemon_config,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Mapping, Sequence
    from contextlib import AbstractContextManager
    from types import TracebackType

    from sg_event_daemon._gaps import IdRun
    from sg_event_daemon._group_types import GroupSnapshot, PreInvocationResult, PreInvocationStep

_ACQUISITION_BUDGET = 10.0
_CATCHUP_WINDOW = 120.0
_CELLS = {
    'catchup-2-empty': 45.0,
    'catchup-13-empty': 30.0,
    'second-process': 25.0,
    'matching-1': 25.0,
    'matching-2': 25.0,
    'matching-13': 25.0,
    'offered-1': 60.0,
    'offered-2': 60.0,
    'offered-13': 60.0,
    'profile-13': 25.0,
    'large-transactions': 0.0,
    'outside-reader-13': 60.0,
}
_CHECKPOINT_BOUND_LABELS = {
    'wal-growth': 'WAL growth',
    'incomplete-duration': 'incomplete duration',
}
_COMMIT_KINDS = (
    'pre-invocation',
    'done',
    'failure',
    'irrelevant-advance',
    'obligation-completion',
    'strict-epoch',
    'source',
    'other',
)
_CONNECT = sqlite3.connect
_DELIVERY_COMMIT_LIMIT = 2
_EXACT_QUERY_TERMS = 3
_HOLD_DEADLINE = 5.0
_LOCAL = threading.local()
_MATCHING_FLOORS = {'matching-1': 1325, 'matching-2': 575, 'matching-13': 90}
_OUTSIDE_GROUPS = 13
_OUTSIDE_SPAWNS = 2
_PAGE = 100
_REUSE_FRACTION = 0.9
_SHM_HEADER_BYTES = 96
_STARTUP_DEADLINE = 30.0
_WAL_READERS: dict[Path, tuple[ExitStack, mmap.mmap | None]] = {}
_WAL_READERS_LOCK = threading.Lock()


class _OutsideOptions(TypedDict, total=False):
    observations: _Observations
    stall_timeout_secs: float | None
    wait_for_pause: bool


class _Handle:
    """Supply an offline callback handle with deterministic exact-event refetches."""

    def __init__(self) -> None:
        """Expose the request settings applied by the production provider adapter."""
        self.config = _HandleConfig()

    def close(self) -> None:
        """Release the resource-free offline handle."""

    def find(
        self, entity_type: str, filters: object, fields: list[str] | None = None, **options: object
    ) -> list[dict[str, object]]:
        """Refetch only the deterministic exact event requested by lane resume.

        Args:
            entity_type: Requested entity type.
            filters: Exact event selector in the form `[['id', 'is',
                event_id]]`.
            fields: Requested event fields.
            **options: Query options supplied by the production recovery path.

        Returns:
            One deterministic event mapping for the selected integer ID.

        Raises:
            ValueError: The query does not request `EventLogEntry`, all
                `EVENT_FIELDS`, one exact integer ID, and options `limit=1`,
                `order=None`, and `page=0`.
        """
        if (
            entity_type != 'EventLogEntry'
            or fields != list(EVENT_FIELDS)
            or options != {'limit': 1, 'order': None, 'page': 0}
        ):
            raise ValueError('unexpected offline recovery query')
        if not isinstance(filters, list) or len(cast('list[object]', filters)) != 1:
            raise ValueError('recovery query must select one exact event')
        clause = cast('list[object]', filters)[0]
        if not isinstance(clause, list) or len(cast('list[object]', clause)) != _EXACT_QUERY_TERMS:
            raise ValueError('recovery query must select one exact event')
        terms = cast('list[object]', clause)
        event_id = terms[2]
        if terms[:2] != ['id', 'is'] or type(event_id) is not int:
            raise ValueError('recovery query must select one exact event')
        return [dict(_event(event_id))]

    def set_session_uuid(self, _session_uuid: str | None) -> None:
        """Accept callback session context without external I/O."""


class _HandleConfig:
    """Hold the adapter's request settings without a network transport."""

    max_rpc_attempts = 0
    rpc_attempt_interval = 0.0
    timeout_secs = 1.0


class _CellOptions(TypedDict):
    """Describe reproducible cell execution and provenance."""

    work_dir: Path
    results_dir: Path
    duration_scale: float
    device_class: str
    filesystem: str
    commit: str
    floors: NotRequired[Literal['gate', 'report']]


class _SummaryOptions(TypedDict, total=False):
    """Describe the offered schedule and delivery observation deadline."""

    drain_end: float | None
    offered_rate: float | None


class _WriteOptions(TypedDict):
    """Describe the growth bound, scaled only in diagnostic tests."""

    limit: int


@dataclass
class _Observations:
    """Keep actual cell observations alive independently of fallible result construction."""

    records: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    stage: str = 'setup'


class _WorkloadOptions(TypedDict):
    """Describe one bounded production-path workload."""

    groups: int
    matching: bool
    duration: float
    checkpoint_interval: NotRequired[float]
    event_limit: NotRequired[int | None]
    offered_rate: NotRequired[float | None]
    second_process: NotRequired[bool]
    profile: NotRequired[bool]
    limit: NotRequired[int]
    observations: NotRequired[_Observations]
    stall_timeout_secs: NotRequired[float]


@dataclass
class _FailureBoundary:
    """Publish a component failure before ending its measurement owner."""

    metrics: dict[str, Any]
    stop: threading.Event

    def __enter__(self) -> None:
        """Enter the measured component's failure boundary."""

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        trace: TracebackType | None,
    ) -> bool:
        """Preserve exception evidence and stop peers before cleanup."""
        if isinstance(error, WriteLockStoppedError) and self.stop.is_set():
            return True
        if error is not None:
            self.metrics['errors'].append(''.join(traceback.format_exception(kind, error, trace)))
            self.stop.set()
        return True


class _ConnectOptions(TypedDict, total=False):
    """Describe the production open_database connection arguments."""

    isolation_level: Literal['DEFERRED', 'EXCLUSIVE', 'IMMEDIATE'] | None
    timeout: float
    uri: bool


class _LockOptions(TypedDict, total=False):
    """Describe the production transaction observation boundary."""

    budget_secs: float
    stop: threading.Event
    timing: WriteTiming
    watch: Callable[[WritePhase, float], AbstractContextManager[None]]
    slice_secs: float


class _CheckpointOptions(TypedDict):
    """Describe CheckpointHost's factory arguments."""

    interval_secs: float
    logger: logging.Logger
    log_extra: Mapping[str, str]


class _Connection(sqlite3.Connection):
    """Observe reset time without replacing any production checkpoint policy."""

    def commit(self) -> None:
        """Observe a successful commit before its production writer lock releases.

        Raises:
            sqlite3.Error: SQLite cannot commit the transaction.
            OSError: Sampling the committed WAL or WAL-index file fails.
        """
        super().commit()
        if getattr(_LOCAL, 'metrics', None) is not None:
            _LOCAL.committed_wal = {
                'at': time.monotonic(),
                'wal_file': wal_file_bytes(_LOCAL.path),
                'wal_in_use': _position(_LOCAL.path),
            }

    def execute(self, sql: str, parameters: object = (), /) -> sqlite3.Cursor:
        """Execute SQL and record successful TRUNCATE checkpoint duration.

        Args:
            sql: SQL statement passed to the underlying connection.
            parameters: Sequence or mapping bound to the statement's
                placeholders.

        Returns:
            The cursor returned by the underlying connection.

        Raises:
            sqlite3.Error: SQLite cannot execute the statement.
        """
        started = time.monotonic()
        result = super().execute(sql, cast('Any', parameters))
        metrics = getattr(_LOCAL, 'metrics', None)
        if metrics is not None and sql == 'PRAGMA wal_checkpoint(TRUNCATE)':
            metrics['resets'].append(time.monotonic() - started)
        return result


class _Lock(SourceWriteLock):
    """Observe the production lock's own disjoint timing boundaries."""

    @contextmanager
    def transaction(
        self, connection: sqlite3.Connection, **options: Unpack[_LockOptions]
    ) -> Generator[sqlite3.Connection]:
        """Run an arbitrated transaction while recording its benchmark timings.

        Args:
            connection: Open connection to the guarded database, configured
                with `isolation_level=None` on the calling thread.
            **options: `budget_secs` overrides the acquisition budget; `stop`
                interrupts acquisition; `timing` receives phase boundaries;
                `watch` reports production write phases; `slice_secs` caps each
                SQLite acquisition wait in seconds.

        Yields:
            The connection while its immediate transaction is active. Normal
            exit commits; an error rolls back before releasing the locks.

        Raises:
            WriteLockTimeoutError: WAL growth or a spent acquisition budget
                prevents the write from starting.
            WriteLockStoppedError: The stop event interrupts acquisition.
            DatabaseError: The lock is closed, this thread already holds the
                lock, or a SQLite transaction operation fails.
            SecurityError: A contended acquisition cannot securely open its
                additional lock-file descriptor.
            OSError: WAL inspection or an operating-system lock operation
                fails.
            RuntimeError: The file-lock waiting thread cannot be started.
            ValueError: The acquisition budget or SQLite wait slice is not
                finite and positive.
        """
        metrics = getattr(_LOCAL, 'metrics', None)
        timing = options.get('timing', WriteTiming())
        options['timing'] = timing
        committed = False
        _LOCAL.committed_wal = None
        profile = None
        try:
            with super().transaction(connection, **options):
                if metrics is not None and metrics['profile_next']:
                    metrics['profile_next'] = False
                    profile = cProfile.Profile()
                    profile.enable()
                yield connection
            committed = True
        except WriteLockTimeoutError as error:
            if metrics is not None:
                metrics['expired'].append({'at': time.monotonic(), 'level': error.level})
            raise
        finally:
            if profile is not None:
                profile.disable()
            if metrics is not None:
                if math.isfinite(timing.finished_at):
                    metrics['transactions'].append(
                        {
                            'at': timing.finished_at,
                            'kind': getattr(_LOCAL, 'kind', 'other'),
                            'acquisition': timing.begun_at - timing.started_at,
                            'protected': timing.finished_at - timing.begun_at,
                            'committed': committed,
                            **(_LOCAL.committed_wal or {'wal_file': 0, 'wal_in_use': None}),
                        }
                    )
                if profile is not None:
                    metrics['profile'] = _profile_shares(profile)


class _Group(GroupState):
    """Label public delivery operations while retaining their production bodies."""

    @staticmethod
    def _record_completion(
        metrics: dict[str, Any] | None, transaction_index: int, snapshot: GroupSnapshot
    ) -> None:
        """Record one matching event at its own observed transaction commit time."""
        if metrics is None or len(metrics['transactions']) <= transaction_index:
            return
        transaction = metrics['transactions'][transaction_index]
        if transaction['committed']:
            # This workload has exactly one callback per matching event.
            metrics['progress'].append(
                {'at': transaction['at'], 'count': 1, 'cursor': snapshot.cursor}
            )

    def advance_irrelevant(self, high_id: int, *, expected_revision: int) -> GroupSnapshot:
        """Observe irrelevant-advance commits while preserving the production operation.

        Args:
            high_id: Inclusive greatest irrelevant event id.
            expected_revision: Required compare-and-swap revision.

        Returns:
            Committed immutable snapshot.

        Raises:
            InvalidTransitionError: Recovery blocks advancement, `high_id` does
                not advance the cursor, or a strict fence would be crossed.
            RevisionConflictError: The expected revision differs.
            DatabaseError: SQLite fails, write acquisition is stopped, or a
                bounded operator or one-attempt acquisition expires.
            SecurityError: The source write-lock path fails safety checks.
            OSError: WAL inspection or write-lock acquisition or release fails.
            RuntimeError: The file-lock acquisition helper thread cannot start.
            ValueError: The stored callback manifest is invalid.
        """
        with _kind('irrelevant-advance'):
            return super().advance_irrelevant(high_id, expected_revision=expected_revision)

    def advance_irrelevant_prefix(self, high_id: int, *, expected_revision: int) -> GroupSnapshot:
        """Observe irrelevant-advance commits while preserving the production operation.

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
            DatabaseError: SQLite fails, write acquisition is stopped, or a
                bounded operator or one-attempt acquisition expires.
            SecurityError: The source write-lock path fails safety checks.
            OSError: WAL inspection or write-lock acquisition or release fails.
            RuntimeError: The file-lock acquisition helper thread cannot start.
            ValueError: The stored callback manifest is invalid.
        """
        with _kind('irrelevant-advance'):
            return super().advance_irrelevant_prefix(high_id, expected_revision=expected_revision)

    def callback_done(self, ordinal: int, *, expected_revision: int) -> GroupSnapshot:
        """Observe done commits while preserving the production operation.

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
            DatabaseError: SQLite fails, write acquisition is stopped, or a
                bounded operator or one-attempt acquisition expires.
            SecurityError: The source write-lock path fails safety checks.
            OSError: WAL inspection or write-lock acquisition or release fails.
            RuntimeError: The file-lock acquisition helper thread cannot start.
            ValueError: The stored callback manifest is invalid.
        """
        metrics = getattr(_LOCAL, 'metrics', None)
        transaction_index = len(metrics['transactions']) if metrics is not None else 0
        with _kind('done'):
            snapshot = super().callback_done(ordinal, expected_revision=expected_revision)
        self._record_completion(metrics, transaction_index, snapshot)
        return snapshot

    def callback_failed(
        self, ordinal: int, failure_text: str, *, expected_revision: int
    ) -> GroupSnapshot:
        """Observe failure commits while preserving the production operation.

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
            DatabaseError: SQLite fails, write acquisition is stopped, or a
                bounded operator or one-attempt acquisition expires.
            SecurityError: The source write-lock path fails safety checks.
            OSError: WAL inspection or write-lock acquisition or release fails.
            RuntimeError: The file-lock acquisition helper thread cannot start.
            ValueError: The stored callback manifest is invalid.
        """
        with _kind('failure'):
            return super().callback_failed(
                ordinal, failure_text, expected_revision=expected_revision
            )

    def commit_pre_invocation(
        self, steps: Sequence[PreInvocationStep], *, expected_revision: int
    ) -> PreInvocationResult:
        """Observe pre-invocation commits while preserving the production operation.

        Args:
            steps: Ordered reservations, event starts, attempts, or late
                completions.
            expected_revision: Required initial revision; a mismatch rejects
                the entire batch.

        Returns:
            The committed snapshot and a semantic rejection, if any. Earlier
            permitted steps remain durable; capacity exhaustion stops after the
            reservation. A leading callback completion instead defines the
            rollback boundary for every following step.

        Raises:
            RevisionConflictError: The initial expected revision differs.
            DatabaseError: SQLite fails, write acquisition is stopped, or a
                bounded operator or one-attempt acquisition expires.
            SecurityError: The source write-lock path fails safety checks.
            OSError: WAL inspection or write-lock acquisition or release fails.
            RuntimeError: The file-lock acquisition helper thread cannot start.
            ValueError: The stored callback manifest is invalid.
        """
        metrics = getattr(_LOCAL, 'metrics', None)
        transaction_index = len(metrics['transactions']) if metrics is not None else 0
        with _kind('pre-invocation'):
            result = super().commit_pre_invocation(steps, expected_revision=expected_revision)
        if (
            steps
            and isinstance(steps[0], CallbackDoneStep)
            and result.snapshot.revision > expected_revision
        ):
            self._record_completion(metrics, transaction_index, result.snapshot)
        return result

    def complete_epoch(
        self, epoch_id: int, cursor: int, *, expected_revision: int
    ) -> GroupSnapshot:
        """Observe strict-epoch commits while preserving the production operation.

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
            DatabaseError: SQLite fails, write acquisition is stopped, or a
                bounded operator or one-attempt acquisition expires.
            SecurityError: The source write-lock path fails safety checks.
            OSError: WAL inspection or write-lock acquisition or release fails.
            RuntimeError: The file-lock acquisition helper thread cannot start.
            ValueError: The stored callback manifest is invalid.
        """
        with _kind('strict-epoch'):
            return super().complete_epoch(epoch_id, cursor, expected_revision=expected_revision)

    def complete_obligation_scan(
        self, obligation_id: str, *, excused: Sequence[IdRun] = ()
    ) -> GroupSnapshot:
        """Observe obligation-completion commits while preserving the production operation.

        Args:
            obligation_id: Range whose complete post-close scan finished.
            excused: Ascending runs of the exact unfinished IDs given up.

        Returns:
            Completed snapshot, or the current halted snapshot without a write.

        Raises:
            InvalidTransitionError: The range is not closed and pending, or the
                unfinished IDs differ from the excused set.
            RevisionConflictError: The group revision changed before
                transactional completion.
            DatabaseError: SQLite fails, write acquisition is stopped, or a
                bounded operator or one-attempt acquisition expires.
            SecurityError: The source write-lock path fails safety checks.
            OSError: WAL inspection or write-lock acquisition or release fails.
            RuntimeError: The file-lock acquisition helper thread cannot start.
            ValueError: The stored callback manifest is invalid.
            sqlite3.Error: Reading durable group state fails.
        """
        with _kind('obligation-completion'):
            return super().complete_obligation_scan(obligation_id, excused=excused)


class _Warnings(logging.Handler):
    """Count warning transitions by checkpoint bound and writer component."""

    def __init__(self, metrics: dict[str, Any]) -> None:
        """Bind the process observation record."""
        super().__init__()
        self._metrics = metrics

    def emit(self, record: logging.LogRecord) -> None:
        """Retain warning ownership without embedding local paths in results."""
        if record.levelno < logging.WARNING:
            return
        message = record.getMessage()
        if message.startswith('checkpoint '):
            category = 'checkpoint:' + message.split(' bound exceeded', 1)[0].removeprefix(
                'checkpoint '
            )
        else:
            category = 'writer:' + str(getattr(record, 'component', 'unknown'))
        self._metrics['warnings'].append({'at': time.monotonic(), 'category': category})


def _admit_page(
    source: SourceState, ordinary: list[int], late_low: int, late_high: int, clock: float
) -> str | None:
    """Commit a source page and its late fill under the production retry boundary."""
    source.begin_cycle()
    admission = source.commit_observations(ordinary, successful_clock=clock)
    if admission.pause is not None:
        raise RuntimeError('source admission paused below the retention limits')
    retained = next(
        (
            item
            for item in source.snapshot().ranges
            if item.low_id == late_low and item.high_id == late_high
        ),
        None,
    )
    if retained is None:
        return None
    source.record_missing_rows(
        retained.obligation_id, list(range(late_low, late_high + 1)), successful_clock=clock
    )
    return retained.obligation_id


def _caught_up_groups(path: Path) -> dict[str, dict[str, int]]:
    """Observe durable cursors and obligations without changing production state."""
    with closing(_CONNECT(path, isolation_level=None)) as connection:
        frontier = connection.execute('SELECT frontier FROM source_meta').fetchone()[0]
        return {
            name: {'cursor': cursor, 'frontier': frontier, 'pending_ranges': pending}
            for name, cursor, pending in connection.execute(
                'SELECT s.group_name, s.cursor, (SELECT COUNT(*) FROM range_groups r '
                'JOIN groups g USING (group_name) WHERE r.group_name = s.group_name '
                "AND r.obligation_state = 'pending' AND r.group_generation = g.group_generation) "
                'FROM group_state s'
            )
        }


def _checkpoint_factory(metrics: dict[str, Any], limit: int) -> Callable[..., Checkpointer]:
    """Observe the actual CheckpointHost attempts without replacing maintenance."""

    class Observed(Checkpointer):
        """Record production checkpoint attempts for one benchmark run.

        Use the instance on its database connection's owning thread and call
        `close()` before closing that connection. Successful attempts append
        their outcomes, WAL size, and crossed bounds to the benchmark metrics.
        """

        def run_once(self) -> CheckpointAttempt:
            """Run production maintenance and record the completed attempt.

            Returns:
                The production checkpoint result after its measurements have
                been appended to the benchmark metrics.

            Raises:
                DatabaseError: A SQLite checkpoint operation fails.
                OSError: Inspecting or synchronizing the WAL fails.
            """
            _LOCAL.metrics = metrics
            result = super().run_once()
            metrics['checkpoints'].append(
                {
                    'outcome': result.outcome,
                    'reuse': result.reuse,
                    'result': result.result,
                    'at': result.finished_at,
                    'wal_file': result.wal_bytes,
                    'crossed': sorted(self.crossed),
                }
            )
            return result

    def create(
        connection: sqlite3.Connection, path: Path, **options: Unpack[_CheckpointOptions]
    ) -> Checkpointer:
        return Observed(connection, path, **options, wal_limit_bytes=limit)

    return create


def _close_positions() -> None:
    """Release observer mappings only after this process's SQLite owners close."""
    for lifetime, _mapping in _WAL_READERS.values():
        lifetime.close()
    _WAL_READERS.clear()


def _connect(database: str, **options: Unpack[_ConnectOptions]) -> sqlite3.Connection:
    """Install the observational connection in this measurement process."""
    return _CONNECT(database, **options, factory=_Connection)


def _event(event_id: int) -> Event:
    """Make a complete immutable offline provider event."""
    payload: dict[str, object] = {str(key): None for key in EVENT_FIELDS}
    payload.update(
        id=event_id,
        type='EventLogEntry',
        event_type='Shotgun_Asset_Change',
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        meta={},
    )
    return Event(payload)


def _json_rows(path: Path) -> list[dict[str, Any]]:
    """Read flushed observations, retaining complete rows after an abrupt kill."""
    if not path.exists():
        return []
    lines = path.read_text().splitlines(keepends=True)
    return [json.loads(line) for line in lines if line.endswith('\n')]


def _kill_outside_peak(
    original: WorkerSpawn,
    path: Path,
    root: Path,
    recovery: dict[str, Any],
    timeout: float,
) -> int:
    """Kill the original worker and retain the durable frontier for replacement."""
    original.process.send_signal(signal.SIGKILL)
    killed = original.process.wait(timeout=timeout)
    retained = _caught_up_groups(path)
    frontier = next(iter(retained.values()))['frontier']
    recovery['retained_frontier'] = frontier
    (root / 'retained-frontier').write_text(str(frontier))
    return killed


@contextmanager
def _kind(name: str) -> Generator[None]:
    """Label one production operation and restore its enclosing label."""
    previous = getattr(_LOCAL, 'kind', 'other')
    _LOCAL.kind = name
    try:
        yield
    finally:
        _LOCAL.kind = previous


def _lane(
    state: GroupState,
    name: str,
    metrics: dict[str, Any],
    *,
    matching: bool,
    stall_timeout_secs: float | None = None,
) -> GroupLane:
    """Construct a production lane with an empty or no-op matching plugin."""
    group = GroupConfig(
        name=name,
        source='events',
        plugins=(),
        callback_credentials='source',
        worker='default',
        execution='sync',
        ordering='eager',
    )
    if stall_timeout_secs is not None:
        group = replace(group, callback_deadline_secs=stall_timeout_secs)
    credential = CredentialConfig(
        name='source', script_name='offline', api_key=SecretValue('offline')
    )
    if stall_timeout_secs is not None:
        # This offline handle does no RPC. Its nominal request budget feeds
        # GroupLane's derived internal stall detector.
        credential = replace(
            credential,
            timeout_secs=max(
                credential.timeout_secs,
                stall_timeout_secs / (2 * credential.max_rpc_attempts),
            ),
        )
    site = SiteConfig(
        name='bench',
        server='https://example.invalid',
        default_source='events',
        credentials={'source': credential},
        sources={'events': SourceConfig(name='events', credentials='source')},
        groups={name: group},
        workers={},
    )

    def factory(site: str, **_options: object) -> _Handle:
        del site
        return _Handle()

    def callback(_context: EventContext) -> None:
        metrics['callbacks'].append(time.monotonic())

    identity = CallbackIdentity(
        ordinal=0,
        plugin_id='noop',
        callback_id='noop',
        execution='sync',
        filter_digest='all',
        config_digest='empty',
        secret_refs=(),
        credential_name='source',
        credential_fingerprint='offline',
    )
    plugins = (
        (
            PluginSpec(
                id='noop',
                module_name='offline',
                credentials='source',
                config={},
                secrets=(),
                setup=None,
                callbacks=(
                    CallbackSpec(id='noop', callback=callback, events=None, identity=identity),
                ),
            ),
        )
        if matching
        else ()
    )
    return GroupLane(
        state=state,
        site=site,
        group=group,
        provider=Provider(
            name='shotgun-api3',
            capabilities=ProviderCapabilities(sync=True, async_=False),
            sync_factory=factory,
            async_factory=None,
        ),
        plugins=plugins,
        worker_instance='bench',
        health_interval_secs=2,
        watchdog=ComponentWatchdog(
            kind='lane',
            identity=name,
            worker_instance='bench',
            registration_token=name,
            startup_deadline_secs=30 if stall_timeout_secs is None else stall_timeout_secs,
        ),
    )


def _load_event_limit(options: _WorkloadOptions) -> int | None:
    """Bound offered admission by the full scheduled count, including its partial page."""
    limit = options.get('event_limit')
    rate = options.get('offered_rate')
    if rate is None:
        return limit
    due = math.floor(rate * options['duration'])
    return min(limit, due) if limit is not None else due


def _measure_large_operations(
    source: SourceState, state: GroupState, metrics: dict[str, Any]
) -> list[dict[str, Any]]:
    """Time each required production FULL operation as one protected transaction."""
    transactions: list[dict[str, Any]] = []
    for name, operation in (
        ('retire-500000-ids', source.begin_cycle),
        (
            'delete-50000-late-ids',
            lambda: state.complete_obligation_scan('deleted_late_ids'),
        ),
    ):
        before = wal_file_bytes(_LOCAL.path)
        with _kind('source'):
            operation()
        observation = metrics['transactions'][-1]
        transactions.append(
            {
                'name': name,
                'protected_seconds': observation['protected'],
                'wal_file_before': before,
                'wal_file_after': wal_file_bytes(_LOCAL.path),
                'growth_bound_bytes': WAL_IN_USE_LIMIT_BYTES,
            }
        )
    return transactions


def _metrics(role: str) -> dict[str, Any]:
    """Create one process or component's raw observation record."""
    return {
        'role': role,
        'transactions': [],
        'expired': [],
        'callbacks': [],
        'progress': [],
        'checkpoints': [],
        'resets': [],
        'warnings': [],
        'errors': [],
        'samples': [],
        'profile_next': False,
        'profile': None,
    }


def _attach_outside_processes(
    result: dict[str, Any], recovery: dict[str, Any], sampled: list[dict[str, Any]]
) -> None:
    """Retain the reader and replacement process observations alongside the killed worker."""
    result['worker_failures'].extend(recovery['errors'])
    recovery_raw = recovery.pop('raw')
    samples = recovery_raw['samples'] + [
        row for record in recovery_raw.get('production_records', []) for row in record['samples']
    ]
    if samples:
        result['peak_open_descriptors']['restarted-site'] = max(row['fds'] for row in samples)
    result['peak_open_descriptors']['outside-reader'] = max(row['fds'] for row in sampled)
    records = result['raw']
    records.append(recovery_raw)
    observer = _metrics('outside-reader')
    observer['samples'] = [
        {'at': row['at'], 'fds': row['fds'], 'wal_file': row['bytes'], 'wal_in_use': None}
        for row in sampled
    ]
    records.append(observer)


def _new_run(
    groups: int, *, second_process: bool, stall_timeout_secs: float | None = None
) -> dict[str, Any]:
    """Allocate the process-shared coordination and per-owner observations."""
    context = multiprocessing.get_context('spawn')
    multiprocessing.set_executable(sys.executable)
    metrics = _metrics('site')
    return {
        'context': context,
        'metrics': metrics,
        'records': [metrics],
        'live_records': [metrics],
        'stop': context.Event() if second_process else threading.Event(),
        'started': context.Event() if second_process else threading.Event(),
        'ready': context.Queue() if second_process else queue.Queue[str](),
        'outbox': context.Queue() if second_process else queue.Queue[dict[str, Any]](),
        'inboxes': [
            context.Queue(maxsize=2) if second_process else queue.Queue[object](maxsize=2)
            for _ in range(groups)
        ],
        'returned': set(),
        'conditions': {'source': ConditionFlags()},
        'workers': [],
        'sampler_done': threading.Event(),
        'start': 0.0,
        'end': 0.0,
        'stall_timeout_secs': stall_timeout_secs,
    }


def _next_page(low: int, options: _WorkloadOptions) -> tuple[int, int, int, list[int]]:
    """Build the next admitted page with a genuine late interval."""
    remaining = options.get('event_limit')
    size = min(_PAGE, remaining - low + 101) if remaining is not None else _PAGE
    high = low + size - 1
    late_low, late_high = high - max(1, size // 10), high - 1
    if size == 1:
        late_low, late_high = (low, low) if remaining is not None else (high + 1, high)
    ordinary = [value for value in range(low, high + 1) if not late_low <= value <= late_high]
    return high, late_low, late_high, ordinary


def _open_descriptor_count() -> int:
    """Count live descriptor numbers after the enumeration's own handle closes."""
    directory = Path('/proc/self/fd') if Path('/proc/self/fd').exists() else Path('/dev/fd')
    descriptors = 0
    for entry in list(directory.iterdir()):
        name = entry.name
        if name.isdecimal():
            try:
                os.fstat(int(name))
            except OSError:
                continue
            descriptors += 1
    return descriptors


def _ordered_times(record: dict[str, Any], names: tuple[str, ...]) -> bool:
    """Require finite same-host observations in their causal order."""
    values: list[float] = []
    for name in names:
        value = record.get(name)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            return False
        values.append(value)
    return all(left <= right for left, right in pairwise(values))


def _attached_replacement(recovery: dict[str, Any], expected: set[str]) -> bool:
    """Check retained authenticated attachment proof without repeating inventory validation."""
    health = recovery.get('catchup_health')
    if not isinstance(health, dict):
        return False
    health = cast('dict[str, Any]', health)
    worker = health.get('worker')
    groups = health.get('groups')
    if not isinstance(worker, dict) or not isinstance(groups, list):
        return False
    worker = cast('dict[str, Any]', worker)
    groups = cast('list[Any]', groups)
    return (
        worker.get('worker_id') == recovery.get('replacement_worker_id')
        and worker.get('spawn_instance') == recovery.get('replacement_spawn_instance')
        and worker.get('monitor_alive') is True
        and 'watchdog_failure' in worker
        and worker['watchdog_failure'] is None
        and _ordered_times(
            {**recovery, 'heartbeat_at': worker.get('heartbeat_at')},
            ('replacement_started_at', 'heartbeat_at', 'ready_at'),
        )
        and len(groups) == _OUTSIDE_GROUPS
        and all(type(row) is dict and isinstance(row.get('group'), str) for row in groups)
        and {row['group'] for row in groups} == expected
        and all(
            row.get('worker_instance') == recovery.get('replacement_spawn_instance')
            and row.get('join_phase') == 'attached'
            and all(
                type(row.get(name)) is int and row[name] == 0
                for name in (
                    'pending_catchup_epoch_count',
                    'outstanding_late_obligation_count',
                    'outstanding_late_id_count',
                )
            )
            for row in groups
        )
    )


def _outside_config(root: Path, groups: tuple[str, ...], scale: float) -> Path:
    """Select full-size production cadence while retaining scaled stress cadence."""
    interval = max(0.02, scale)
    config = write_daemon_config(
        root,
        Topology(
            'default',
            groups,
            health_interval_secs=interval,
            supervisor_poll_interval_secs=0.01,
            checkpoint_interval_secs=interval,
            fetch_interval_secs=1.0 if scale == 1 else 0.001,
        ),
    )
    config.write_text(config.read_text().replace('worker_startup_deadline_secs = 10.0\n', ''))
    return config


def _outside_pause_complete(rows: list[dict[str, Any]]) -> bool:
    """Require real growth refusals from the source and every delivery owner."""
    expected = {'sg-event-daemon:source:admin'} | {
        f'sg-event-daemon:lane:group{index}' for index in range(_OUTSIDE_GROUPS)
    }
    paused: set[str] = set()
    for row in rows:
        owner = row['owner']
        if owner not in expected:
            continue
        if row['kind'] == 'pause':
            paused.add(owner)
        elif owner in paused:
            # The outside reader keeps the WAL above the bound. This negative
            # check therefore holds at any speed, without a quiet-time window.
            raise RuntimeError(f'{owner}: activity after growth pause')
    return paused == expected


def _paused_peak(
    records: list[dict[str, Any]],
    sampled: list[dict[str, Any]],
    pinned: float,
    duration: float,
    limit: int,
) -> bool:
    """Require the measured paused growth episode before an intentional recovery kill."""
    pause_at = max(pinned, sampled[-1]['at'] - min(0.2, duration / 5))
    return (
        sampled[-1]['bytes'] > limit
        and not any(
            row['at'] >= pause_at and row['committed']
            for record in records
            for row in record['transactions']
        )
        and not any(at >= pause_at for record in records for at in record['callbacks'])
    )


def _percentiles(values: list[float]) -> dict[str, float]:
    """Return observed p99 and maximum, with zero for an empty sample."""
    ordered = sorted(values)
    return {
        'p99': ordered[max(0, math.ceil(len(ordered) * 0.99) - 1)] if ordered else 0.0,
        'max': max(ordered, default=0.0),
    }


def _pin_outside_reader(reader: sqlite3.Connection, root: Path, observer: dict[str, Any]) -> float:
    """Establish the SQLite read pin before releasing the deterministic load."""
    reader.execute('BEGIN')
    reader.execute('SELECT COUNT(*) FROM source_meta').fetchone()
    pinned = time.monotonic()
    observer['pinned_at'] = pinned
    (root / 'events-ready').touch()
    return pinned


def _position(path: Path) -> int | None:
    """Read stable WAL-index headers without a SQLite read transaction."""
    with _WAL_READERS_LOCK:
        if path not in _WAL_READERS:
            try:
                with ExitStack() as opened:
                    stream = opened.enter_context(Path(f'{path}-shm').open('rb'))
                    lifetime = opened.pop_all()
            except FileNotFoundError:
                return 0
            # Even an incomplete header must retain its descriptor: closing any
            # descriptor for this inode releases this process's SQLite locks.
            _WAL_READERS[path] = lifetime, None
            if os.fstat(stream.fileno()).st_size < _SHM_HEADER_BYTES:
                return None
            mapping = lifetime.enter_context(
                mmap.mmap(stream.fileno(), _SHM_HEADER_BYTES, access=mmap.ACCESS_READ)
            )
            _WAL_READERS[path] = lifetime, mapping
        mapping = _WAL_READERS[path][1]
        if mapping is None:
            return None
        headers = mapping[:_SHM_HEADER_BYTES]
        if headers[:48] == headers[48:] and headers[12]:
            page_size = struct.unpack_from('=H', headers, 14)[0]
            page_size = 65_536 if page_size == 1 else page_size
            return 32 + struct.unpack_from('=I', headers, 16)[0] * (page_size + 24)
    return None


def _production_records(spawn: WorkerSpawn) -> list[dict[str, Any]]:
    """Translate recorded production observations to the existing raw cell shape."""
    records: dict[str, dict[str, Any]] = {}
    for row in _json_rows(spawn.log_path.with_suffix('.storage')):
        role = row.pop('role')
        kind = row.pop('kind')
        record = records.setdefault(role, _metrics(role))
        if kind == 'callbacks':
            record[kind].append(row['at'])
        else:
            if kind == 'transactions':
                row['kind'] = row.pop('kind_label')
            record[kind].append(row)
    return list(records.values())


def _profile_shares(profile: cProfile.Profile) -> dict[str, float]:
    """Split exclusive profile time into SQLite, snapshot Python, and other Python."""
    shares = {'sqlite': 0.0, 'snapshot_reads': 0.0, 'other_python': 0.0}
    for entry in profile.getstats():
        code = entry.code
        filename, function = (
            ('', code) if isinstance(code, str) else (code.co_filename, code.co_name)
        )
        key = (
            'sqlite'
            if 'sqlite3' in function
            else 'snapshot_reads'
            if filename.endswith('_group_store.py') and 'snapshot' in function
            else 'other_python'
        )
        shares[key] += entry.inlinetime
    total = sum(shares.values())
    return {key: value / total for key, value in shares.items()}


def _put_page(inbox: object, page: object, stop: threading.Event) -> None:
    """Publish one bounded page without stranding shutdown behind a full queue."""
    channel = cast('queue.Queue[object]', inbox)
    while not stop.is_set():
        try:
            channel.put(page, timeout=0.1)
        except queue.Full:
            continue
        else:
            return


def _record_admission(runtime: dict[str, Any], high: int) -> None:
    """Keep source admission at the original cutoff, before queue backpressure."""
    if time.monotonic() <= runtime['end']:
        runtime['admitted_at_load_end'] = high - 100


def _admit_load(source: SourceState, options: _WorkloadOptions, runtime: dict[str, Any]) -> int:
    """Retry source acquisition at its polling boundary and preserve page order."""
    low = 101
    stop = runtime['stop']
    event_limit = _load_event_limit(options)
    options = {**options, 'event_limit': event_limit}
    rate = options.get('offered_rate')
    runtime['admitted_at_load_end'] = 0
    while not stop.is_set() and time.monotonic() < runtime['end']:
        if event_limit is not None and low >= 101 + event_limit:
            break
        high, late_low, late_high, ordinary = _next_page(low, options)
        if rate is not None:
            due_at = (
                runtime['end']
                if high - low + 1 < _PAGE
                else runtime['start'] + (high - 100) / rate
            )
            stop.wait(max(0, due_at - time.monotonic()))
            if stop.is_set():
                break
        clock = time.monotonic() - runtime['start'] + 2
        obligation = _admit_page(source, ordinary or [high + 1], late_low, late_high, clock)
        _record_admission(runtime, high)
        for inbox in runtime['inboxes']:
            _put_page(inbox, (low, high, late_low, late_high, obligation, clock), stop)
        low = high + 1
    return low - 101


def _recovered_peak(result: dict[str, Any]) -> bool:
    """Require this episode's owner-open peak and recovery before any production write."""
    recovery = result['recovery']
    peak = result.get('peak_pinned_wal_bytes')
    owner_open = recovery.get('wal_bytes_before_owner_open')
    return (
        type(peak) is int
        and peak > 0
        and type(owner_open) is int
        and owner_open >= peak
        and type(recovery.get('wal_bytes_before_first_write')) is int
        and recovery['wal_bytes_before_first_write'] == 0
        and type(recovery.get('wal_bytes_after_recovery')) is int
        and recovery['wal_bytes_after_recovery'] == 0
        and _ordered_times(
            recovery,
            (
                'replacement_started_at',
                'recovery_started_at',
                'recovery_finished_at',
                'first_actual_write_entered_at',
                'first_write_entered_at',
                'first_write_completed_at',
            ),
        )
    )


def _replacement_caught_up(recovery: dict[str, Any]) -> bool:
    """Require exact durable and attached-health proof inside the separate catch-up window."""
    names = recovery.get('expected_groups')
    expected = {f'group{index}' for index in range(_OUTSIDE_GROUPS)}
    groups = recovery.get('groups')
    if not isinstance(names, list) or not isinstance(groups, dict):
        return False
    names = cast('list[object]', names)
    groups = cast('dict[str, Any]', groups)
    frontier = recovery.get('retained_frontier')
    return (
        recovery.get('all_groups_caught_up') is True
        and len(names) == _OUTSIDE_GROUPS
        and all(isinstance(name, str) for name in names)
        and set(names) == expected
        and type(frontier) is int
        and frontier >= 0
        and set(groups) == expected
        and all(
            type(row) is dict
            and type(row.get('cursor')) is int
            and row['cursor'] == frontier
            and type(row.get('frontier')) is int
            and row['frontier'] == frontier
            and type(row.get('pending_ranges')) is int
            and row['pending_ranges'] == 0
            for row in groups.values()
        )
        and _attached_replacement(recovery, expected)
        and recovery.get('ready_spawn_instance') == recovery.get('replacement_spawn_instance')
        and recovery.get('ready_worker_id') == recovery.get('replacement_worker_id')
        and _ordered_times(recovery, ('startup_complete_at', 'catchup_started_at'))
        and _ordered_times(
            recovery,
            ('first_write_completed_at', 'catchup_started_at', 'ready_at', 'catchup_finished_at'),
        )
        and recovery['catchup_finished_at'] - recovery['catchup_started_at'] < _CATCHUP_WINDOW
    )


def _require(condition: bool, message: str) -> None:
    """Fail a measurement when the production operation violates its contract."""
    if not condition:
        raise RuntimeError(message)


def _deliver_page(state: GroupState, lane: GroupLane, batch: tuple[Any, ...]) -> None:
    """Drive the production ordinary page and separate pending late obligation."""
    low, high, late_low, late_high, obligation, clock = batch
    _progress, pending = state.refresh_ranges()
    state.snapshot()
    ordinary = [value for value in range(low, high + 1) if not late_low <= value <= late_high]
    snapshot, consumed = lane.deliver_many_counted(Delivery(_event(value)) for value in ordinary)
    _require(
        consumed == len(ordinary) and snapshot.status == 'running',
        'ordinary lane failed to consume its admitted page',
    )
    if obligation is not None:
        _require(pending is not None, 'admitted obligation missing from group refresh')
        revision = next(item for item in pending or () if item.obligation_id == obligation)
        snapshot, all_consumed = lane.deliver_obligation_rows(
            revision,
            (_event(value) for value in range(late_low, late_high + 1)),
            source_successful_clock=clock,
        )
        _require(
            snapshot.status == 'running' and all_consumed,
            'late lane failed to consume its admitted page',
        )
        lane.complete_obligation(obligation)


def _consume_pages(
    state: GroupState,
    lane: GroupLane,
    channels: dict[str, Any],
    metrics: dict[str, Any],
    *,
    matching: bool,
) -> None:
    """Record nonmatching page progress without duplicating matching event completions."""
    while not channels['stop'].is_set():
        try:
            batch = channels['inbox'].get(timeout=0.1)
        except queue.Empty:
            continue
        if batch is None:
            break
        _deliver_page(state, lane, batch)
        if not matching:
            metrics['progress'].append(
                {
                    'at': time.monotonic(),
                    'count': batch[1] - batch[0] + 1,
                    'cursor': state.snapshot().cursor,
                }
            )


def _offered_rate(result: dict[str, Any], results_dir: Path, groups: int) -> float:
    """Use the matching cell's measured source rate with identical run provenance."""
    dependency = f'matching-{groups}'
    matched = json.loads((results_dir / f'{dependency}.json').read_text())
    for key in ('qualification', 'duration_scale', 'device_class', 'filesystem', 'commit'):
        _require(matched[key] == result[key], f'matching dependency differs in {key}')
    _require(matched['cell'] == dependency, 'matching dependency has the wrong cell identity')
    _require(
        not matched.get('execution_failed') and not matched.get('incomplete'),
        'matching dependency did not complete',
    )
    floor_missed = (
        matched.get('floor_missed') is True
        or matched.get('criteria', {}).get('source-events-rate') is False
    )
    rates: list[float] = []
    if floor_missed:
        floors = matched.get('floors')
        required_runs = 3
        expected_criteria = {
            'no-worker-failure',
            'protected-write-deadline',
            'acquisition-budget',
            'no-bound-crossing',
            'no-expired-attempt',
            'reuse-at-least-90-percent',
            'peak-wal-file',
            'source-events-rate',
            'delivery-commits-per-callback',
        }
        _require(
            floors in {'gate', 'report'}
            and matched.get('floor_missed') is True
            and expected_criteria.issubset(matched.get('criteria', {}))
            and matched.get('criteria', {}).get('source-events-rate') is False
            and all(
                passed
                for name, passed in matched['criteria'].items()
                if name != 'source-events-rate'
            )
            and matched.get('failed_criteria')
            == (['source-events-rate'] if floors == 'gate' else [])
            and matched.get('passed') is (floors == 'report'),
            'matching dependency has an unusable floor verdict',
        )
        raw_rates = matched.get('run_rates')
        raw_runs = matched.get('runs')
        _require(
            matched.get('attempt_count') == required_runs
            and isinstance(raw_rates, list)
            and len(cast('list[object]', raw_rates)) == required_runs
            and isinstance(raw_runs, list)
            and len(cast('list[object]', raw_runs)) == required_runs,
            'matching dependency lacks required runs',
        )
        rates = cast('list[float]', raw_rates)
        runs = cast('list[dict[str, Any]]', raw_runs)
        for run, rate in zip(runs, rates, strict=True):
            _require(
                all(
                    run.get(key) == matched[key]
                    for key in (
                        'cell',
                        'qualification',
                        'duration_scale',
                        'device_class',
                        'filesystem',
                        'commit',
                        'floors',
                    )
                )
                and not run.get('execution_failed')
                and not run.get('incomplete')
                and expected_criteria.issubset(run.get('criteria', {}))
                and all(
                    passed
                    for name, passed in run.get('criteria', {}).items()
                    if name != 'source-events-rate'
                )
                and type(rate) in (int, float)
                and math.isfinite(rate)
                and rate > 0
                and run.get('metrics', {}).get('source_events_per_second') == rate,
                'matching dependency has an unusable run',
            )
    _require(
        matched.get('passed') or floor_missed,
        'qualification requires a passing matching dependency',
    )
    measured = matched.get('median_source_events_per_second')
    _require(
        type(measured) in (int, float) and math.isfinite(measured) and measured > 0,
        'matching dependency has no measured throughput',
    )
    if floor_missed:
        _require(
            median(rates) == measured,
            'matching dependency median differs from its runs',
        )
    offered = measured * 0.8
    _require(
        math.isfinite(offered) and offered > 0, 'matching dependency has no measured throughput'
    )
    result.update(matching_dependency=dependency, offered_source_events_per_second=offered)
    return offered


def _retain_outside_pause(original: WorkerSpawn, observer: dict[str, Any]) -> None:
    """Retain and validate the original spawn's complete observation barrier."""
    evidence = _json_rows(original.log_path.with_suffix('.pause'))
    observer['pause_evidence'] = evidence
    if not _outside_pause_complete(evidence):
        raise RuntimeError('outside-reader pause barrier incomplete')


def _retain_paused_peak(
    probe: SupervisorProbe,
    original: WorkerSpawn,
    observer: dict[str, Any],
    sampled: list[dict[str, Any]],
    window: tuple[float, float, int],
) -> set[str]:
    """Retain authenticated original health and reject an invalid recovery episode."""
    pinned, duration, limit = window
    pause_evidence = observer.get('pause_evidence')
    health = probe.latest_health[original.worker_id]
    observer['original_health'] = asdict(health)
    observer['samples'] = sampled
    records = _production_records(original)
    paused = (
        _paused_peak(records, sampled, pinned, duration, limit)
        if pause_evidence is None
        else sampled[-1]['bytes'] > limit and _outside_pause_complete(pause_evidence)
    )
    if not paused:
        raise RuntimeError('outside-reader did not establish an over-bound paused peak')
    conditions = {condition for source in health.sources for condition in source.conditions}
    conditions.update(condition for group in health.groups for condition in group.conditions)
    return conditions


def _retain_restart_evidence(replacement: WorkerSpawn, recovery: dict[str, Any]) -> None:
    """Cache completed owner recovery and the first successful production write."""
    if 'recovery_finished_at' not in recovery:
        recovered = _json_rows(replacement.log_path.with_suffix('.wal-recovery'))
        if not recovered:
            return
        row = recovered[0]
        recovery.update(
            recovery_started_at=row['started'],
            recovery_finished_at=row['finished'],
            wal_bytes_before_owner_open=row['before'],
            wal_bytes_after_recovery=row['after'],
            wal_recovery_seconds=row['finished'] - row['started'],
        )
    if 'first_write_completed_at' not in recovery:
        writes = _json_rows(replacement.log_path.with_suffix('.wal-writes'))
        entered = next((row for row in writes if row['phase'] == 'begin'), None)
        completed = next((row for row in writes if row['phase'] == 'commit'), None)
        if entered is not None:
            recovery.setdefault('first_actual_write_entered_at', entered['at'])
        if completed is not None:
            entered = next(
                (
                    row
                    for row in writes
                    if row['phase'] == 'begin' and row.get('started') == completed.get('started')
                ),
                None,
            )
        if entered is not None:
            recovery.update(
                first_write_entered_at=entered['at'],
                wal_bytes_before_first_write=entered['bytes'],
            )
        if completed is not None:
            recovery.update(
                first_write_completed_at=completed['at'],
                first_write_seconds=completed['at'] - replacement.started_at,
            )


def _retain_worker_record(runtime: dict[str, Any], record: dict[str, Any]) -> None:
    """Retain process results without duplicating already-owned local observations."""
    runtime['returned'].add(record['role'])
    # Local lanes retain the authoritative object before starting; even their
    # results become copies when the shared outbox is a multiprocessing queue.
    if not any(existing['role'] == record['role'] for existing in runtime['records']):
        runtime['records'].append(record)


def _sample_until_stopped(path: Path, metrics: dict[str, Any], done: threading.Event) -> None:
    """Retain the sampler's observations until its SQLite owner finishes."""
    while True:
        metrics['samples'].append(
            {
                'at': time.monotonic(),
                'fds': _open_descriptor_count(),
                'wal_file': wal_file_bytes(path),
                'wal_in_use': _position(path),
            }
        )
        if done.wait(0.05):
            return


def _sample(path: Path, metrics: dict[str, Any], done: threading.Event) -> None:
    """Sample this process's descriptors and WAL every fifty milliseconds."""
    with _FailureBoundary(metrics, done):
        _sample_until_stopped(path, metrics, done)


def _seed_large_transactions(connection: sqlite3.Connection, counts: dict[str, int]) -> None:
    """Create the required retained populations before the timed production operations."""
    for name, count in counts.items():
        connection.execute(
            'INSERT INTO discovery_ranges (obligation_id, low_id, high_id, '
            'opened_successful_at, '
            'visibility_deadline, source_revision, close_generation, range_state, '
            'scan_next_id, scan_pages, degraded) '
            'VALUES (?, 1000, ?, 0, 300, 1, 1, ?, 1000, 0, 0)',
            (name, 999 + count, 'closed'),
        )
        connection.executemany(
            'INSERT INTO source_late_ids VALUES (?, ?)',
            ((name, value) for value in range(1000, 1000 + count)),
        )
    connection.execute(
        'INSERT INTO range_groups VALUES (?, ?, 1, 100, 0, ?)',
        ('deleted_late_ids', 'group0', 'pending'),
    )
    connection.executemany(
        'INSERT INTO late_ids (group_name, obligation_id, event_id, terminal_state) '
        'VALUES (?, ?, ?, ?)',
        (
            ('group0', 'deleted_late_ids', value, 'done')
            for value in range(1000, 1000 + counts['deleted_late_ids'])
        ),
    )


def _set_verdict(result: dict[str, Any], criteria: dict[str, bool]) -> None:
    """Keep the worst non-floor verdict while making a reported floor non-gating."""
    for run in result.get('runs', []):
        for name, passed in run['criteria'].items():
            if name != 'source-events-rate':
                criteria[name] = criteria.get(name, True) and passed
    result['criteria'] = criteria
    result['failed_criteria'] = [
        name
        for name, passed in criteria.items()
        if not passed and not (name == 'source-events-rate' and result.get('floors') == 'report')
    ]
    result['passed'] = not result['failed_criteria']


def _source(
    path: Path, writes: WriteContext, *, create: bool, synchronous: str = 'NORMAL'
) -> SourceState:
    """Open the real source authority with the requested primary durability."""
    return SourceState.open(
        path,
        writes=writes,
        create=create,
        site='bench',
        source='events',
        stream_fingerprint='offline',
        source_generation='source-1',
        synchronous=synchronous,
    )


def _group(
    path: Path,
    name: str,
    writes: WriteContext,
    *,
    resume: bool = False,
    synchronous: str = 'NORMAL',
) -> GroupState:
    """Open a real eager group on the shared source database."""
    options = (
        {}
        if resume
        else {
            'initial_cursor': 100,
            'initial_delivery_floor': 100,
            'initial_last_delivered_id': 100,
        }
    )
    return _Group.open(
        path,
        path.parent / f'{name}.lock',
        writes=writes,
        site='bench',
        site_server_url='https://example.invalid',
        source='events',
        group=name,
        group_generation=1,
        ordering='eager',
        stream_fingerprint='offline',
        source_generation='source-1',
        verified_incarnation='incarnation-1',
        start_mode='resume' if resume else 'initialize',
        synchronous=synchronous,
        **options,
    )


@contextmanager
def _measurement_environment(interval: float) -> Generator[None]:
    """Restore connection observation and test-only timing on every exit path."""
    previous_connect = sqlite3.connect
    previous_wait = _checkpoint.WAL_REUSE_WAIT_SECS
    sqlite3.connect = _connect
    _checkpoint.WAL_REUSE_WAIT_SECS = 0.1 * interval
    try:
        yield
    finally:
        sqlite3.connect = previous_connect
        _checkpoint.WAL_REUSE_WAIT_SECS = previous_wait
        _LOCAL.metrics = None


def _stall_timeout(runtime: dict[str, Any], production: float) -> float:
    """Use an optional test workload timeout without changing CLI measurements."""
    override = runtime.get('stall_timeout_secs')
    return production if override is None else override


def _join_workers(runtime: dict[str, Any]) -> None:
    """Bound cleanup while preserving actual child exit evidence."""
    for worker in runtime['workers']:
        worker.join(timeout=_stall_timeout(runtime, 30))
        if worker.is_alive():
            runtime['metrics']['errors'].append('worker did not stop')
            if isinstance(worker, BaseProcess):
                worker.kill()
                worker.join()
        if isinstance(worker, BaseProcess) and worker.exitcode != 0:
            runtime['metrics']['errors'].append(f'worker exit {worker.exitcode}')


def _drain_load(runtime: dict[str, Any]) -> None:
    """Collect every lane's drain commits before stopping observers."""
    _LOCAL.metrics = None
    for inbox in runtime['inboxes']:
        _put_page(inbox, None, runtime['stop'])
    for _ in runtime['workers']:
        try:
            record = runtime['outbox'].get(timeout=_stall_timeout(runtime, 60))
            _retain_worker_record(runtime, record)
        except queue.Empty:
            runtime['metrics']['errors'].append('worker did not return its observations')
            runtime['stop'].set()
            break
    _join_workers(runtime)
    runtime['sampler_done'].set()
    runtime['sampler'].join()


def _settle_workload(runtime: dict[str, Any]) -> None:
    """Stop and collect every live owner before its surrounding SQLite lifetime ends."""
    runtime['stop'].set()
    runtime['started'].set()
    deadline = time.monotonic() + _stall_timeout(runtime, 30)
    while len(runtime['returned']) < len(runtime['workers']) and time.monotonic() < deadline:
        try:
            record = runtime['outbox'].get(timeout=0.05)
        except queue.Empty:
            if not any(worker.is_alive() for worker in runtime['workers']):
                break
        else:
            _retain_worker_record(runtime, record)
    _join_workers(runtime)
    runtime['sampler_done'].set()
    sampler = runtime.get('sampler')
    if sampler is not None:
        sampler.join()


def _start_load(path: Path, options: _WorkloadOptions, runtime: dict[str, Any]) -> None:
    """Coordinate the exact component load window."""
    runtime['sampler'] = threading.Thread(
        target=_sample, args=(path, runtime['metrics'], runtime['sampler_done'])
    )
    runtime['sampler'].start()
    runtime['start'] = time.monotonic()
    runtime['end'] = runtime['start'] + options['duration']
    runtime['started'].set()
    _LOCAL.metrics = runtime['metrics']
    _LOCAL.kind = 'source'


def _summarize(
    records: list[dict[str, Any]],
    start: float,
    end: float,
    groups: int,
    **options: Unpack[_SummaryOptions],
) -> dict[str, Any]:
    """Keep load-window throughput separate from all commits including drain."""
    drain_end = options.get('drain_end')
    drain_end = end if drain_end is None else drain_end
    offered_rate = options.get('offered_rate')
    transactions = [row for record in records for row in record['transactions']]
    callbacks = [at for record in records for at in record['callbacks']]
    commits = Counter(row['kind'] for row in transactions if row['committed'])
    attempts = [row for record in records for row in record['checkpoints']]
    samples = [row for record in records for row in record['samples']]
    longest = run = 0
    for row in attempts:
        run = run + 1 if row['reuse'] == 'reuse-busy' else 0
        longest = max(longest, run)
    group_results: dict[str, dict[str, Any]] = {}
    for record in records:
        if not record['role'].startswith('group'):
            continue
        progress = record['progress']
        group_results[record['role']] = {
            'delivered_at_load_end': sum(row['count'] for row in progress if row['at'] <= end),
            'delivered_at_drain_end': sum(
                row['count'] for row in progress if row['at'] <= drain_end
            ),
            'delivered_including_drain': sum(row['count'] for row in progress),
        }
        if offered_rate is not None:
            done = sorted(row['at'] for row in progress)
            # Each lane finishes the ordinary and late events of one page
            # before starting the next; event IDs need not be in commit order.
            lag = [
                at - min(start + (index // _PAGE + 1) * _PAGE / offered_rate, end)
                for index, at in enumerate(done)
            ]
            group_results[record['role']]['delivery_lag_seconds'] = {
                'median': median(lag) if lag else 0.0,
                **_percentiles(lag),
            }
    total_delivered = sum(row['delivered_at_load_end'] for row in group_results.values())
    return {
        'source_events_per_second': total_delivered / groups / (end - start),
        'matched_callbacks_per_second': sum(start <= at <= end for at in callbacks)
        / (end - start),
        'callbacks': len(callbacks),
        'commits': {kind: commits[kind] for kind in _COMMIT_KINDS},
        'delivery_commits_per_callback': sum(
            commits[kind] for kind in ('pre-invocation', 'done', 'failure')
        )
        / len(callbacks)
        if callbacks
        else 0.0,
        'reuse': {
            'reused': sum(row['reuse'] == 'reused' for row in attempts),
            'busy': sum(row['reuse'] == 'reuse-busy' for row in attempts),
            'longest_consecutive_busy': longest,
        },
        'reset_seconds': _percentiles([value for record in records for value in record['resets']]),
        'peak_wal_in_use_bytes': max(
            (row['wal_in_use'] or 0 for row in samples + transactions), default=0
        ),
        'peak_wal_file_bytes': max(
            [row['wal_file'] for row in samples + transactions + attempts], default=0
        ),
        'acquisition_seconds': _percentiles([row['acquisition'] for row in transactions]),
        'protected_write_seconds': _percentiles([row['protected'] for row in transactions]),
        'peak_open_descriptors': {
            record['role']: max(row['fds'] for row in record['samples'])
            for record in records
            if record['samples']
        },
        'worker_failures': [error for record in records for error in record['errors']],
        'bound_crossings': sum(
            row['category'].startswith(('writer:', 'checkpoint:'))
            for record in records
            for row in record['warnings']
        ),
        'expired_attempts': sum(len(record['expired']) for record in records),
        'groups': group_results,
        'profile_shares': next(
            (record['profile'] for record in records if record['profile'] is not None), None
        ),
        'raw': records,
    }


def _warning_expectations(records: list[dict[str, Any]]) -> dict[str, int]:
    """Count required warnings from independent bound and writer-wait transitions."""
    expected: Counter[str] = Counter()
    for record in records:
        crossed: set[str] = set()
        for attempt in record['checkpoints']:
            current = set(attempt['crossed'])
            expected.update(
                'checkpoint:' + _CHECKPOINT_BOUND_LABELS[bound] for bound in current - crossed
            )
            crossed = current
        component = 'source' if record['role'] == 'site' else record['role']
        events = [(row['at'], False) for row in record['expired']]
        events.extend((row['at'], True) for row in record['transactions'] if row['committed'])
        stalled = False
        for _at, succeeded in sorted(events):
            if succeeded:
                stalled = False
            elif not stalled:
                expected['writer:' + component] += 1
                stalled = True
    return dict(expected)


def _outside_summary(
    records: list[dict[str, Any]],
    sampled: list[dict[str, Any]],
    recovery: dict[str, Any],
    recovery_raw: dict[str, Any],
    episode: dict[str, Any],
) -> dict[str, Any]:
    """Apply the original pause, overshoot, and warning accounting to production records."""
    pinned, duration = episode['pinned'], episode['duration']
    pin_seconds, limit = episode['pin_seconds'], episode['limit']
    killed, conditions = episode['killed'], episode['conditions']
    result = _summarize(records, pinned, pinned + duration, _OUTSIDE_GROUPS)
    peak = max(row['bytes'] for row in sampled)
    crossed = next((row for row in sampled if row['bytes'] > limit), None)
    transactions = [row for record in records for row in record['transactions']]
    pause_at = sampled[-1]['at'] - min(0.2, duration / 5)
    paused = not any(
        row['at'] >= pause_at and row['committed'] for row in transactions
    ) and not any(at >= pause_at for record in records for at in record['callbacks'])
    if episode.get('pause_evidence') is not None:
        paused = _outside_pause_complete(episode['pause_evidence'])
        result['pause_evidence'] = episode['pause_evidence']
    last_activity = max(
        [pinned]
        + [row['at'] for row in transactions if row['committed']]
        + [at for record in records for at in record['callbacks']]
    )
    crossing_commit = min(
        (row['at'] for row in transactions if row['wal_file'] > limit), default=math.inf
    )
    excess = {
        record['role']: sum(
            row['committed'] and row['at'] > crossing_commit for row in record['transactions']
        )
        for record in records
    }
    warnings = Counter(row['category'] for record in records for row in record['warnings'])
    result.update(
        reader_pin_seconds=pin_seconds,
        growth_bound_bytes=limit,
        crossing_wal_bytes=None if crossed is None else crossed['bytes'],
        peak_pinned_wal_bytes=peak,
        commits_after_crossing_by_writer=excess,
        delivery_paused=paused,
        delivery_quiet_seconds=max(0.0, sampled[-1]['at'] - last_activity),
        stall_conditions=sorted(conditions & {'checkpoint-stalled', 'write-lock-stalled'}),
        warnings=dict(warnings),
        warning_expectations=_warning_expectations(records),
        recovery=recovery,
        killed_exitcode=killed,
    )
    recovery['raw'] = recovery_raw
    _attach_outside_processes(result, recovery, sampled)
    return result


def _outside_warnings_observed(records: list[dict[str, Any]]) -> bool:
    """Wait for every crossing and warning required by the CI episode assertions."""
    crossed = {
        bound
        for record in records
        for attempt in record['checkpoints']
        for bound in attempt['crossed']
    }
    warnings = Counter(row['category'] for record in records for row in record['warnings'])
    # Completion requires the observations, not a passing verdict. Duplicate
    # warnings must reach the final assertions and fail.
    return (
        {'wal-growth', 'incomplete-duration'} <= crossed
        and {'checkpoint:WAL growth', 'checkpoint:incomplete duration'} <= warnings.keys()
        and any(category.startswith('writer:') for category in warnings)
        and all(
            warnings[category] >= count
            for category, count in _warning_expectations(records).items()
        )
    )


def _outside_pause_ready(
    probe: SupervisorProbe, original: WorkerSpawn, sample: dict[str, Any], limit: int
) -> bool:
    """End CI on growth, stalls, owner refusals and complete warning observations."""
    health = probe.latest_health.get(original.worker_id)
    if health is None or sample['bytes'] <= limit:
        return False
    conditions = {condition for source in health.sources for condition in source.conditions}
    conditions.update(condition for group in health.groups for condition in group.conditions)
    return (
        {'checkpoint-stalled', 'write-lock-stalled'} <= conditions
        and _outside_pause_complete(_json_rows(original.log_path.with_suffix('.pause')))
        and _outside_warnings_observed(_production_records(original))
    )


def _within_startup(
    recovery: dict[str, Any], field: str, deadline_secs: float = _STARTUP_DEADLINE
) -> bool:
    """Measure each startup observation from the parent's pre-spawn clock."""
    if not _ordered_times(recovery, ('replacement_started_at', field)):
        return False
    return 0 <= recovery[field] - recovery['replacement_started_at'] < deadline_secs


def _replacement_observations(
    probe: SupervisorProbe,
    original: WorkerSpawn,
    path: Path,
    recovery: dict[str, Any],
    *,
    stall_timeout_secs: float | None = None,
) -> None:
    """Observe startup and first write together, then durable catch-up and service readiness."""
    startup_deadline = _STARTUP_DEADLINE if stall_timeout_secs is None else stall_timeout_secs
    catchup_window = _CATCHUP_WINDOW if stall_timeout_secs is None else stall_timeout_secs
    probe.wait_for(
        lambda: len(probe.spawns) > 1,
        15 if stall_timeout_secs is None else stall_timeout_secs,
    )
    replacement = probe.spawns[1]
    recovery.update(
        replacement_worker_id=replacement.worker_id,
        replacement_spawn_instance=replacement.spawn_instance,
        replacement_started_at=replacement.started_at,
    )

    def require_current() -> None:
        if (
            replacement.process.poll() is not None
            or len(probe.spawns) != _OUTSIDE_SPAWNS
            or any(item.spawn_instance != original.spawn_instance for item in probe.terminations)
        ):
            raise RuntimeError('replacement worker failed')

    def observe() -> bool:
        require_current()
        now = time.monotonic()
        if 'startup_complete_at' not in recovery and probe.startup_complete(
            replacement.worker_id, replacement.spawn_instance, now=now
        ):
            recovery.update(
                startup_complete_at=now,
                startup_spawn_instance=replacement.spawn_instance,
                startup_worker_id=replacement.worker_id,
            )
        _retain_restart_evidence(replacement, recovery)
        return all(
            name in recovery
            for name in ('startup_complete_at', 'first_write_completed_at', 'recovery_finished_at')
        )

    probe.wait_for(observe, max(0.0, replacement.started_at + startup_deadline - time.monotonic()))
    if not all(
        _within_startup(recovery, field, startup_deadline)
        for field in ('startup_complete_at', 'first_write_completed_at')
    ):
        raise RuntimeError('replacement startup or first write missed deadline')
    recovery['catchup_started_at'] = time.monotonic()
    expected = set(recovery['expected_groups'])

    def caught_up() -> bool:
        require_current()
        now = time.monotonic()
        recovery['groups'] = _caught_up_groups(path)
        health = probe.latest_health.get(replacement.worker_id)
        current = probe.startup_complete(
            replacement.worker_id, replacement.spawn_instance, now=now
        )
        if health is not None:
            recovery['catchup_health'] = asdict(health)
        durable = set(recovery['groups']) == expected and all(
            row['cursor'] == row['frontier'] == recovery['retained_frontier']
            and row['pending_ranges'] == 0
            for row in recovery['groups'].values()
        )
        attached = (
            current
            and health is not None
            and {group.group for group in health.groups} == expected
            and all(
                group.worker_instance == replacement.spawn_instance
                and group.join_phase == 'attached'
                and group.pending_catchup_epoch_count == 0
                and group.outstanding_late_obligation_count == 0
                and group.outstanding_late_id_count == 0
                for group in health.groups
            )
        )
        ready = probe.supervisor.probe_result().readiness
        recovery['all_groups_caught_up'] = durable and attached and ready
        if recovery['all_groups_caught_up']:
            # The returned result and its persisted JSON share one representation.
            recovery['catchup_health'] = json.loads(json.dumps(recovery['catchup_health']))
            recovery.update(
                ready_at=now,
                ready_spawn_instance=replacement.spawn_instance,
                ready_worker_id=replacement.worker_id,
            )
        return recovery['all_groups_caught_up']

    probe.wait_for(caught_up, catchup_window)
    require_current()
    recovery['catchup_finished_at'] = time.monotonic()
    if recovery['catchup_finished_at'] - recovery['catchup_started_at'] >= catchup_window:
        raise RuntimeError('replacement catch-up missed deadline')
    recovery['catchup_finished_seconds'] = recovery['catchup_finished_at'] - replacement.started_at


def _outside_reader(
    path: Path,
    duration: float,
    scale: float,
    **options: Unpack[_OutsideOptions],
) -> dict[str, Any]:
    """Pin a production worker, kill its paused peak, and observe its supervised replacement."""
    observations = options.get('observations') or _Observations()
    stall_timeout_secs, wait_for_pause = (
        options.get('stall_timeout_secs'),
        options.get('wait_for_pause', False),
    )
    observations.stage = 'outside-reader'
    root = ensure_secure_state_dir(path.parent)
    config = _outside_config(
        root, tuple(f'group{index}' for index in range(_OUTSIDE_GROUPS)), scale
    )
    path = source_database(root / 'state', site='test', source='admin')
    limit = (
        WAL_IN_USE_LIMIT_BYTES
        if scale == 1
        else max(1_000_000, int(WAL_IN_USE_LIMIT_BYTES * scale * scale))
    )
    sampled: list[dict[str, Any]] = []
    recovery: dict[str, Any] = {
        'all_groups_caught_up': False,
        'groups': {},
        'errors': [],
        'expected_groups': [f'group{index}' for index in range(_OUTSIDE_GROUPS)],
    }
    recovery_raw = _metrics('restarted-site')
    recovery_raw['recovery'] = recovery
    observations.records.append(recovery_raw)
    observer = _metrics('outside-reader')
    observations.records.append(observer)
    with running_supervisor(
        config,
        ChildPatches(
            writer_wal_limit_bytes=limit,
            probe_wal_restart=True,
            outside_reader=True,
            outside_pause_observation=wait_for_pause,
            outside_frontier_cap=500 if scale == 1 / 16 else None,
            hold_deadline_secs=_HOLD_DEADLINE,
        ),
    ) as probe:
        probe.wait_for(
            lambda: probe.supervisor.probe_result().readiness,
            30 if stall_timeout_secs is None else stall_timeout_secs,
        )
        original = probe.spawns[0]
        recovery['original_spawn_instance'] = original.spawn_instance
        with closing(_CONNECT(path, isolation_level=None)) as reader:
            pinned = _pin_outside_reader(reader, root, observer)

            def sample() -> bool:
                sampled.append(
                    {
                        'at': time.monotonic(),
                        'bytes': wal_file_bytes(path),
                        'fds': _open_descriptor_count(),
                    }
                )
                if original.process.poll() is not None or probe.terminations:
                    raise RuntimeError('outside-reader original worker failed')
                if wait_for_pause:
                    return _outside_pause_ready(probe, original, sampled[-1], limit)
                return time.monotonic() - pinned >= duration

            try:
                probe.wait_for(
                    sample,
                    None
                    if wait_for_pause
                    else duration + 5
                    if stall_timeout_secs is None
                    else stall_timeout_secs,
                )
                if wait_for_pause:
                    _retain_outside_pause(original, observer)
                conditions = _retain_paused_peak(
                    probe, original, observer, sampled, (pinned, duration, limit)
                )
                killed = _kill_outside_peak(
                    original,
                    path,
                    root,
                    recovery,
                    5 if stall_timeout_secs is None else stall_timeout_secs,
                )
                if wait_for_pause:
                    # Re-read after exit: records flushed between the last
                    # acknowledgment and SIGKILL must also satisfy the verdict.
                    _retain_outside_pause(original, observer)
                pin_seconds = time.monotonic() - pinned
                reader.execute('COMMIT')
                observations.stage = 'recovery'
                _replacement_observations(
                    probe, original, path, recovery, stall_timeout_secs=stall_timeout_secs
                )
            except (AssertionError, RuntimeError, OSError, TimeoutError) as error:
                recovery['errors'].append(str(error))
                raise
            finally:
                observer['samples'] = sampled
                records = _production_records(original)
                observations.records.extend(records)
                if len(probe.spawns) > 1:
                    replacement_records = _production_records(probe.spawns[1])
                    observations.records.extend(replacement_records)
                    recovery_raw['production_records'] = replacement_records
            return _outside_summary(
                records,
                sampled,
                recovery,
                recovery_raw,
                {
                    'conditions': conditions,
                    'pinned': pinned,
                    'duration': duration,
                    'pin_seconds': pin_seconds,
                    'limit': limit,
                    'killed': killed,
                    'pause_evidence': observer.get('pause_evidence'),
                },
            )


def _replacement_startup(recovery: dict[str, Any]) -> bool:
    """Reject absent, stale, failed, or late authenticated replacement startup."""
    return (
        all(
            isinstance(recovery.get(name), str) and recovery[name]
            for name in (
                'original_spawn_instance',
                'replacement_spawn_instance',
                'replacement_worker_id',
            )
        )
        and recovery.get('replacement_spawn_instance') != recovery.get('original_spawn_instance')
        and recovery.get('startup_spawn_instance') == recovery.get('replacement_spawn_instance')
        and bool(recovery.get('replacement_worker_id'))
        and recovery.get('startup_worker_id') == recovery.get('replacement_worker_id')
        and recovery.get('errors') == []
        and _within_startup(recovery, 'startup_complete_at')
    )


def _replacement_write(recovery: dict[str, Any]) -> bool:
    """Require a finite first-write duration consistent with the shared pre-spawn clock."""
    elapsed = recovery.get('first_write_seconds')
    return (
        isinstance(elapsed, (int, float))
        and not isinstance(elapsed, bool)
        and math.isfinite(elapsed)
        and 0 <= elapsed < _STARTUP_DEADLINE
        and _within_startup(recovery, 'first_write_completed_at')
        and elapsed == recovery['first_write_completed_at'] - recovery['replacement_started_at']
    )


def _assess(result: dict[str, Any]) -> None:
    """Evaluate unmodified qualification criteria, even for diagnostic scaling."""
    metrics = result['metrics']
    cell = result['cell']
    if result.get('execution_failed'):
        result['criteria'] = {'no-worker-failure': False, 'execution-completed': False}
        result['failed_criteria'] = list(result['criteria'])
        result['passed'] = False
        return
    criteria = {
        'no-worker-failure': not metrics['worker_failures'],
        'protected-write-deadline': metrics['protected_write_seconds']['max'] <= _HOLD_DEADLINE,
        'acquisition-budget': metrics['acquisition_seconds']['max'] <= _ACQUISITION_BUDGET,
    }
    if cell != 'outside-reader-13':
        criteria.update(
            {
                'no-bound-crossing': metrics['bound_crossings'] == 0
                and metrics['peak_wal_file_bytes'] <= WAL_IN_USE_LIMIT_BYTES,
                'no-expired-attempt': metrics['expired_attempts'] == 0,
            }
        )
    if cell.startswith(('catchup-', 'matching-')) or cell == 'second-process':
        reuse = metrics['reuse']
        attempts = reuse['reused'] + reuse['busy']
        criteria['reuse-at-least-90-percent'] = (
            attempts > 0 and reuse['reused'] / attempts >= _REUSE_FRACTION
        )
        criteria['peak-wal-file'] = metrics['peak_wal_file_bytes'] <= 64 * 1024 * 1024
    if cell.startswith('matching-'):
        floor = _MATCHING_FLOORS[cell]
        rate = result.get('median_source_events_per_second', metrics['source_events_per_second'])
        criteria['source-events-rate'] = rate >= floor
        result['floor_missed'] = not criteria['source-events-rate']
        criteria['delivery-commits-per-callback'] = (
            metrics['callbacks'] > 0
            and metrics['delivery_commits_per_callback'] <= _DELIVERY_COMMIT_LIMIT
        )
    if cell.startswith('offered-'):
        scheduled = result['offered_source_events_per_second'] * result['duration_seconds']
        result['offered'] = math.floor(scheduled)
        released = _PAGE * max(0, math.ceil(scheduled / _PAGE) - 1)
        for group, row in result['groups'].items():
            row['offered_due_at_load_end'] = result['offered']
            row['released_at_load_end'] = released
            row['backlog_at_load_end'] = max(0, released - row['delivered_at_load_end'])
            row['undelivered_at_load_end'] = result['offered'] - row['delivered_at_load_end']
            criteria[f'{group}-99-percent-delivery'] = (
                row['delivered_at_drain_end'] >= result['offered'] * 0.99
            )
            criteria[f'{group}-two-page-backlog'] = row['backlog_at_load_end'] <= 2 * _PAGE
    if cell == 'large-transactions':
        for row in result['large_transactions']:
            criteria[row['name'] + '-full-commit-max'] = row['protected_seconds'] <= 1.0
    if cell == 'outside-reader-13':
        recovery = result['recovery']
        criteria.update(
            {
                'reader-pinned-for-duration': result['reader_pin_seconds']
                >= result['duration_seconds'],
                'growth-crossed': result['crossing_wal_bytes'] is not None,
                'both-stall-conditions': set(result['stall_conditions'])
                == {'checkpoint-stalled', 'write-lock-stalled'},
                'delivery-paused': result['delivery_paused'],
                'one-transaction-per-writer-overshoot': all(
                    count <= 1 for count in result['commits_after_crossing_by_writer'].values()
                ),
                'warning-per-transition': bool(result['warning_expectations'])
                and result['warnings'] == result['warning_expectations'],
                'replacement-startup-within-deadline': _replacement_startup(recovery),
                'recovery-truncated-before-write': _recovered_peak(result),
                'recovery-first-write-startup-deadline': _replacement_write(recovery),
                'all-groups-caught-up': _replacement_caught_up(recovery),
            }
        )
    _set_verdict(result, criteria)


def _writes(
    path: Path,
    stop: threading.Event,
    conditions: ConditionFlags,
    logger: logging.Logger,
    component: str,
    **options: Unpack[_WriteOptions],
) -> WriteContext:
    """Use the default production acquisition budget and WAL pause policy."""
    return WriteContext(
        _Lock(
            path.with_suffix('.db.write-lock'),
            database_path=path,
            timeout_secs=10,
            wait_phase_grace_secs=2,
            wal_limit_bytes=options['limit'],
        ),
        stop=stop,
        conditions=conditions,
        logger=logger,
        log_extra={'component': component},
        policy='one-attempt' if component == 'source' else 'wait',
    )


def _group_worker(
    path: Path, settings: dict[str, Any], channels: dict[str, Any], shared: dict[str, Any]
) -> None:
    """Own one group connection through setup, page delivery, and complete cleanup."""
    name = settings['name']
    metrics = _metrics(name) if shared['metrics'] is None else shared['metrics']
    logger = logging.getLogger(f'storage-load.{name}')
    handler = _Warnings(metrics)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    sampler_done = threading.Event()
    sampler = None
    _LOCAL.path = path
    announced = False
    with _FailureBoundary(metrics, channels['stop']), ExitStack() as stack:
        stack.callback(logger.removeHandler, handler)
        if settings['remote']:
            stack.enter_context(_measurement_environment(1.0))
        conditions = ConditionFlags()
        if shared['conditions'] is not None:
            shared['conditions'][name] = conditions
        state = stack.enter_context(
            _group(
                path,
                name,
                _writes(path, channels['stop'], conditions, logger, name, limit=settings['limit']),
            )
        )
        lane = stack.enter_context(
            closing(
                _lane(
                    state,
                    name,
                    metrics,
                    matching=settings['matching'],
                    stall_timeout_secs=settings['stall_timeout_secs'],
                )
            )
        )
        _require(lane.setup() is None, 'lane setup failed')
        if settings['remote']:
            sampler = threading.Thread(target=_sample, args=(path, metrics, sampler_done))
            sampler.start()
        channels['ready'].put(name)
        announced = True
        _require(channels['started'].wait(_stall_timeout(settings, 30)), 'workload start barrier')
        _LOCAL.metrics = metrics
        metrics['profile_next'] = settings['profile']
        _consume_pages(state, lane, channels, metrics, matching=settings['matching'])
    _LOCAL.metrics = None
    sampler_done.set()
    if sampler is not None:
        sampler.join()
        _close_positions()
    if not announced:
        channels['ready'].put('failed: ' + repr(metrics['errors']))
    channels['outbox'].put(metrics)


def _start_lanes(path: Path, options: _WorkloadOptions, runtime: dict[str, Any]) -> None:
    """Start lane owners and surface setup errors before beginning the load."""
    for index, inbox in enumerate(runtime['inboxes']):
        remote = options.get('second_process', False) and index == options['groups'] - 1
        factory = runtime['context'].Process if remote else threading.Thread
        metrics = None if remote else _metrics(f'group{index}')
        if metrics is not None:
            runtime['live_records'].append(metrics)
            runtime['records'].append(metrics)
        worker = factory(
            target=_group_worker,
            args=(
                path,
                {
                    'name': f'group{index}',
                    'matching': options['matching'],
                    'limit': options.get('limit', WAL_IN_USE_LIMIT_BYTES),
                    'profile': options.get('profile', False) and index == 0,
                    'remote': remote,
                    'stall_timeout_secs': options.get('stall_timeout_secs'),
                },
                {
                    'inbox': inbox,
                    'ready': runtime['ready'],
                    'started': runtime['started'],
                    'stop': runtime['stop'],
                    'outbox': runtime['outbox'],
                },
                {'metrics': metrics, 'conditions': None if remote else runtime['conditions']},
            ),
        )
        worker.daemon = True
        worker.start()
        runtime['workers'].append(worker)
    for _ in runtime['workers']:
        announcement = runtime['ready'].get(timeout=_stall_timeout(runtime, 30))
        if announcement.startswith('failed: '):
            runtime['started'].set()
            _join_workers(runtime)
            raise RuntimeError(announcement)


def _run_workload(path: Path, **options: Unpack[_WorkloadOptions]) -> dict[str, Any]:
    """Run real source writes and bounded-page lane delivery, then count drain."""
    ensure_secure_state_dir(path.parent)
    runtime = _new_run(
        options['groups'],
        second_process=options.get('second_process', False),
        stall_timeout_secs=options.get('stall_timeout_secs'),
    )
    observations = options.get('observations', _Observations())
    observations.stage = 'setup'
    observations.records.extend(runtime['records'])
    runtime['records'] = observations.records
    metrics = runtime['metrics']
    logger = logging.getLogger('storage-load.source')
    handler = _Warnings(metrics)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    interval = options.get('checkpoint_interval', 1.0)
    limit = options.get('limit', WAL_IN_USE_LIMIT_BYTES)
    _LOCAL.path, _LOCAL.metrics = path, None
    offered = 0

    def failed(error: str) -> None:
        metrics['errors'].append(error)
        runtime['stop'].set()

    try:
        with _measurement_environment(interval), ExitStack() as stack:
            stack.callback(logger.removeHandler, handler)
            source = stack.enter_context(
                _source(
                    path,
                    _writes(
                        path,
                        runtime['stop'],
                        runtime['conditions']['source'],
                        logger,
                        'source',
                        limit=limit,
                    ),
                    create=True,
                )
            )
            source.initialize(100, incarnation='incarnation-1')
            host = CheckpointHost(
                interval_secs=interval,
                synchronous='NORMAL',
                on_failure=failed,
                logger=logger,
                checkpointer_factory=_checkpoint_factory(metrics, limit),
            )
            stack.callback(host.close)
            runtime['view'] = host.register(
                path, source_identity('bench', 'events'), SOURCE_SCHEMA, log_extra={}
            )
            stack.callback(_settle_workload, runtime)
            _start_lanes(path, options, runtime)
            _start_load(path, options, runtime)
            observations.stage = 'load'
            with _FailureBoundary(metrics, runtime['stop']):
                offered = _admit_load(source, options, runtime)
            observations.stage = 'drain'
            _drain_load(runtime)
            observations.stage = 'cleanup'
    finally:
        if not any(worker.is_alive() for worker in runtime['workers']):
            _close_positions()
    observations.stage = 'summary'
    rate = options.get('offered_rate')
    drain_seconds = _PAGE / rate if rate is not None else 0.0
    result = _summarize(
        runtime['records'],
        runtime['start'],
        runtime['end'],
        options['groups'],
        drain_end=runtime['end'] + drain_seconds,
        offered_rate=rate,
    )
    result['drain_seconds'] = drain_seconds
    result['offered'] = math.floor(rate * options['duration']) if rate is not None else offered
    result['admitted'] = offered
    result['admitted_at_load_end'] = runtime['admitted_at_load_end']
    result['load_start'] = runtime['start']
    result['load_end'] = runtime['end']
    return result


def _large_transactions(
    path: Path, scale: float, *, observations: _Observations | None = None
) -> dict[str, Any]:
    """Measure the two required single FULL transactions after unmeasured seeding."""
    observations = observations if observations is not None else _Observations()
    # Exercise the lane path before building the retained populations.
    initial = _run_workload(
        path,
        groups=1,
        matching=True,
        duration=0.1,
        event_limit=1,
        checkpoint_interval=max(0.02, scale),
        observations=observations,
    )
    observations.stage = 'setup'
    metrics = _metrics('large-transactions')
    observations.records.append(metrics)
    logger = logging.getLogger('storage-load.large')
    handler = _Warnings(metrics)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    stop = threading.Event()
    counts = {
        'retired_ids': max(1, int(500_000 * scale)),
        'deleted_late_ids': max(1, int(50_000 * scale)),
    }
    try:
        with _measurement_environment(max(0.02, scale)), ExitStack() as stack:
            stack.callback(logger.removeHandler, handler)
            source = stack.enter_context(
                _source(
                    path,
                    _writes(
                        path,
                        stop,
                        ConditionFlags(),
                        logger,
                        'source',
                        limit=WAL_IN_USE_LIMIT_BYTES,
                    ),
                    create=False,
                    synchronous='FULL',
                )
            )
            state = stack.enter_context(
                _group(
                    path,
                    'group0',
                    _writes(
                        path,
                        stop,
                        ConditionFlags(),
                        logger,
                        'group0',
                        limit=WAL_IN_USE_LIMIT_BYTES,
                    ),
                    resume=True,
                    synchronous='FULL',
                )
            )
            connection = stack.enter_context(closing(_CONNECT(path, isolation_level=None)))
            connection.execute('PRAGMA foreign_keys=ON')
            connection.execute('PRAGMA wal_autocheckpoint=0')
            seed_writes = _writes(
                path, stop, ConditionFlags(), logger, 'seed', limit=WAL_IN_USE_LIMIT_BYTES
            )
            stack.callback(seed_writes.lock.close)
            observations.stage = 'seed'
            with seed_writes.transaction(connection):
                _seed_large_transactions(connection, counts)
            host = CheckpointHost(
                interval_secs=1,
                synchronous='NORMAL',
                on_failure=metrics['errors'].append,
                logger=logger,
                checkpointer_factory=_checkpoint_factory(metrics, WAL_IN_USE_LIMIT_BYTES),
            )
            stack.callback(host.close)
            host.register(path, source_identity('bench', 'events'), SOURCE_SCHEMA, log_extra={})
            done = threading.Event()
            sampler = threading.Thread(target=_sample, args=(path, metrics, done))
            sampler.start()
            start = time.monotonic()
            _LOCAL.metrics = metrics
            observations.stage = 'large-transactions'
            try:
                transactions = _measure_large_operations(source, state, metrics)
            finally:
                done.set()
                sampler.join()
            _require(
                connection.execute(
                    "SELECT COUNT(*) FROM source_late_ids WHERE obligation_id='retired_ids'"
                ).fetchone()
                == (0,),
                'source retirement retained rows',
            )
            _require(
                connection.execute(
                    'SELECT COUNT(*) FROM late_ids WHERE group_name=?', ('group0',)
                ).fetchone()
                == (0,),
                'group late deletion retained rows',
            )
    finally:
        _close_positions()
    observations.stage = 'summary'
    result = _summarize([metrics], start, time.monotonic(), 1)
    result['worker_failures'].extend(initial['worker_failures'])
    result['large_transactions'] = transactions
    result['populations'] = counts
    return result


def _run_attempt(cell: str, **options: Unpack[_CellOptions]) -> dict[str, Any]:
    """Run one cell and retain its observations and explicit criterion verdicts."""
    work_dir, results_dir = options['work_dir'], options['results_dir']
    duration_scale = options['duration_scale']
    path = work_dir / cell / 'source.db'
    if cell == 'outside-reader-13' and (path.parent.exists() or path.parent.is_symlink()):
        raise FileExistsError(f'cell work directory already exists: {path.parent}')
    work_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f'cell work database already exists: {path}')
    results_dir.mkdir(parents=True, exist_ok=True)
    if any((results_dir / f'{cell}{suffix}.json').exists() for suffix in ('', '.raw')):
        raise FileExistsError('cell observations already exist; refusing to overwrite them')
    duration = _CELLS[cell] * duration_scale
    result: dict[str, Any] = {
        'cell': cell,
        'qualification': duration_scale == 1,
        'duration_seconds': duration,
        'duration_scale': duration_scale,
        'device_class': options['device_class'],
        'filesystem': options['filesystem'],
        'commit': options['commit'],
        'floors': options.get('floors', 'gate'),
    }
    failure = _metrics('cell-controller')
    observations = _Observations(records=[failure])
    metrics: dict[str, Any] = {}
    with _FailureBoundary(failure, threading.Event()):
        if cell == 'outside-reader-13':
            metrics = _outside_reader(path, duration, duration_scale, observations=observations)
        elif cell == 'large-transactions':
            metrics = _large_transactions(path, duration_scale, observations=observations)
        else:
            groups = (
                13
                if cell in {'catchup-13-empty', 'profile-13'}
                else 2
                if cell in {'catchup-2-empty', 'second-process'}
                else int(cell.rsplit('-', maxsplit=1)[-1])
            )
            offered = None
            if cell.startswith('offered-'):
                offered = _offered_rate(result, results_dir, groups)
            metrics = _run_workload(
                path,
                groups=groups,
                matching=cell.startswith(('matching-', 'offered-')) or cell == 'profile-13',
                duration=duration,
                checkpoint_interval=max(0.02, duration_scale),
                offered_rate=offered,
                second_process=cell == 'second-process',
                profile=cell == 'profile-13',
                observations=observations,
            )
    if failure['errors']:
        result.update(execution_failed=True, incomplete=True, error_stage=observations.stage)
        metrics = {
            'raw': observations.records,
            'worker_failures': [
                error for record in observations.records for error in record['errors']
            ],
        }
    for key in (
        'groups',
        'profile_shares',
        'large_transactions',
        'populations',
        'offered',
        'admitted',
        'admitted_at_load_end',
        'load_start',
        'load_end',
        'drain_seconds',
        'reader_pin_seconds',
        'growth_bound_bytes',
        'crossing_wal_bytes',
        'peak_pinned_wal_bytes',
        'commits_after_crossing_by_writer',
        'delivery_paused',
        'stall_conditions',
        'warnings',
        'warning_expectations',
        'recovery',
        'killed_exitcode',
    ):
        if key in metrics:
            result[key] = metrics.pop(key)
    raw = metrics.pop('raw')
    result['metrics'] = metrics
    _assess(result)
    (results_dir / f'{cell}.raw.json').write_text(json.dumps(raw) + '\n')
    (results_dir / f'{cell}.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def _run_cell(cell: str, **options: Unpack[_CellOptions]) -> dict[str, Any]:
    """Repeat marginal matching rates while preserving every other criterion."""
    first = _run_attempt(cell, **options)
    runs = [first]
    if cell.startswith('matching-') and not first.get('execution_failed'):
        floor = _MATCHING_FLOORS[cell]
        if first['metrics']['source_events_per_second'] < 1.2 * floor:
            for attempt in (2, 3):
                if any(name != 'source-events-rate' for name in runs[-1]['failed_criteria']):
                    break
                repeated = options.copy()
                repeated['work_dir'] = options['work_dir'] / f'attempt-{attempt}'
                repeated['results_dir'] = options['results_dir'] / f'attempt-{attempt}'
                runs.append(_run_attempt(cell, **repeated))
    result = dict(first)
    result['attempt_count'] = len(runs)
    if cell.startswith('matching-'):
        rates = [
            run['metrics']['source_events_per_second']
            for run in runs
            if 'source_events_per_second' in run['metrics']
        ]
        result['run_rates'] = rates
        if rates:
            result['median_source_events_per_second'] = median(rates)
        result['runs'] = runs
        _assess(result)
    options['results_dir'].mkdir(parents=True, exist_ok=True)
    (options['results_dir'] / f'{cell}.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def _main() -> int:
    """Run requested cells in dependency order and stop at the first miss."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('run',))
    parser.add_argument('--cell', choices=('all', *_CELLS), required=True)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--results-dir', type=Path, required=True)
    parser.add_argument('--device-class', required=True)
    parser.add_argument('--filesystem', required=True)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--duration-scale', type=float, default=1)
    parser.add_argument('--floors', choices=('gate', 'report'), default='gate')
    args = parser.parse_args()
    if not 0 < args.duration_scale <= 1:
        parser.error('duration scale must be in (0, 1]; scaling is not qualification')
    cells = list(_CELLS) if args.cell == 'all' else [args.cell]
    for index, cell in enumerate(cells):
        result = _run_cell(
            cell,
            work_dir=args.work_dir,
            results_dir=args.results_dir,
            duration_scale=args.duration_scale,
            device_class=args.device_class,
            filesystem=args.filesystem,
            commit=args.commit,
            floors=args.floors,
        )
        print(
            json.dumps(
                {
                    'cell': cell,
                    'passed': result['passed'],
                    'failed_criteria': result['failed_criteria'],
                }
            ),
            flush=True,
        )
        if not result['passed']:
            (args.results_dir / 'pending.json').write_text(
                json.dumps({'first_failed_cell': cell, 'pending': cells[index + 1 :]}, indent=2)
                + '\n'
            )
            return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(_main())
