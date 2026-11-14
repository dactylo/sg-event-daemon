"""Measure per-source storage contention for the spec section 9 gate.

Run as `PYTHONPATH=src python benchmarks/storage_contention.py <command>`, or
use `make benchmark-storage`. Commands: `seed` builds a population file, `run`
executes one gate cell, `gate` seeds both populations and runs all twelve
cells, `evaluate` applies the acceptance thresholds to result files, and
`child` is the internal entry point for a remote worker process. Results are
JSON. The harness drives the production write lock, checkpointer, and version 1
source schema with modeled transactions; it does not run daemon workers or
contact ShotGrid.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import logging
import math
import os
import platform
import resource
import select
import shlex
import shutil
import sqlite3
import struct
import sys
import tempfile
import threading
import time
from array import array
from collections import Counter, defaultdict
from collections.abc import Iterator, Sequence
from contextlib import ExitStack, closing, contextmanager, suppress
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast, overload

from sg_event_daemon._checkpoint import CheckpointAttempt, Checkpointer
from sg_event_daemon._checkpoint_host import CheckpointHost
from sg_event_daemon._config import (
    DEFAULT_CHECKPOINT_INTERVAL_SECS,
    DEFAULT_LATE_EVENT_WINDOW_SECS,
    DEFAULT_MAX_GROUP_OBLIGATIONS,
    DEFAULT_MAX_LATE_IDS_PER_GROUP,
    DEFAULT_MAX_OPEN_RANGES,
)
from sg_event_daemon._platform import SecurityError, ensure_secure_state_dir, spawn_child
from sg_event_daemon._source_schema import (
    BUMP_INDEX_REVISION,
    BUMP_RANGE_REVISION,
    SOURCE_SCHEMA,
    UNRESOLVED_LATE_ID_COUNT_SQL,
    UNRESOLVED_LATE_ID_PREFIX_SQL,
    source_identity,
)
from sg_event_daemon._sqlite import (
    WAL_IN_USE_LIMIT_BYTES,
    WAL_TRIGGER_BYTES,
    DatabaseError,
    open_database,
)
from sg_event_daemon._write_lock import (
    SourceWriteLock,
    WriteLockStoppedError,
    WriteLockTimeoutError,
    WriteTiming,
    write_lock_timeout_secs,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable, Mapping
    from types import TracebackType

    from sg_event_daemon._platform import ChildProcess
    from sg_event_daemon._write_lock import WriteLockLevel

_MIB = 1024 * 1024

_AGGREGATED_LATENCY_METRICS = (
    'commit_latency_ms',
    'all_commit_latency_ms',
    'full_lifecycle_latency_ms',
    'acquisition_ms',
    'service_ms',
    'process_local_wait_ms',
    'file_wait_ms',
    'sqlite_wait_ms',
)
_BURST_SECS = 60.0
_BURST_TOTAL_SECS = 300.0
_CAPACITY_GROUP = 'g12'
_CAPACITY_MINIMUMS = {
    'discovery_ranges': 1000,
    'open_ranges': DEFAULT_MAX_OPEN_RANGES,
    'pending:g11': 1000,
    'source_late_ids': 50_000,
    'late_ids': 50_000,
}
_CAPACITY_REPLAY_EVERY = 60
_CATCHUP_GROUPS = ('g09', 'g10', 'g11', 'g12')
_CHECKPOINT_RESULT_FIELDS = 3
_COUNT_SQL = {
    'source_meta': 'SELECT COUNT(*) FROM source_meta',
    'groups': 'SELECT COUNT(*) FROM groups',
    'group_state': 'SELECT COUNT(*) FROM group_state',
    'callback_marks': 'SELECT COUNT(*) FROM callback_marks',
    'discovery_ranges': 'SELECT COUNT(*) FROM discovery_ranges',
    'range_groups': 'SELECT COUNT(*) FROM range_groups',
    'source_late_ids': 'SELECT COUNT(*) FROM source_late_ids',
    'late_ids': 'SELECT COUNT(*) FROM late_ids',
    'replay_markers': 'SELECT COUNT(*) FROM replay_markers',
    'epochs': 'SELECT COUNT(*) FROM epochs',
    'join_commits': 'SELECT COUNT(*) FROM join_commits',
    'audit': 'SELECT COUNT(*) FROM audit',
    'conditional_park_results': 'SELECT COUNT(*) FROM conditional_park_results',
    'disposition_audit': 'SELECT COUNT(*) FROM disposition_audit',
    'stream_adoptions': 'SELECT COUNT(*) FROM stream_adoptions',
    'pending_adoptions': 'SELECT COUNT(*) FROM pending_adoptions',
}
# Covers even all four catch-up lanes' 384,000 writes plus ordinary lanes,
# health reads and periodic/GC spans over 600 seconds. Retries can overflow it.
_DIAGNOSTIC_CAP = 1_000_000
_DIAGNOSTIC_CELL = 6
_DRAIN_SECS = 2.0
_DURABILITY_LIMITS: dict[str, dict[str, float]] = {
    'normal': {'p99': 20.0, 'p999': 100.0, 'max': 500.0},
    'full': {'p99': 50.0, 'p999': 250.0, 'max': 1000.0},
}
_EVENT_RATE = 25.0
_FETCH_INTERVAL_SECS = 1.0
_GAP_PROBABILITY = 0.125
_GROUP_NAMES = tuple(f'g{index:02d}' for index in range(13))
_HEALTH_INTERVAL_SECS = 2.0
_LATE_ID_PROBABILITY = 0.2
_LIMITS: dict[str, float] = {
    'catchup_p99': 50.0,
    'lifecycle_p99': 200.0,
    'achieved_ratio': 0.99,
    'uncleared_intervals': 2.0,
    'wal_file_bytes': 64 * _MIB,
}
_MAX_REPEAT_RUNS = 3
_MERGE_PROBABILITY = 0.1
_MILLISECONDS = 1000.0
_MIN_DURATION_SECS = 600.0
_MIN_SAMPLES = 50_000
_ORPHAN_GROUP = 'orphan'
_PHASE_NAMES = ('begin', 'body', 'commit', 'passive', 'truncate', 'sync')
_PHASE_ROW = struct.Struct('<QQBB6x6d')
_PINNED_GROUPS = ('g11', 'g12')
_RANGE_LIFETIME_SECS = DEFAULT_LATE_EVENT_WINDOW_SECS
_REPEAT_BAND = 0.8
_REPEAT_PENDING_RUNS = 2
_REPLAY_DISTANCE = 10_000
_REPLAY_GROUPS = _GROUP_NAMES[:11]
_RESTORE_CHUNK_RANGES = 18
_RESTORE_INTERVAL_SECS = 0.5
_RSS_KIB = 1024
_SNAPSHOT_BLOB_BYTES = 2_048
_SQL_BIND_RANGE = """
INSERT INTO range_groups (obligation_id, group_name, group_generation, delivery_floor,
 eligibility_point, obligation_state)
SELECT :obligation, g.group_name, g.group_generation, s.delivery_floor,
 :now + g.holdback_secs,
 CASE WHEN s.delivery_floor < :high THEN 'pending' ELSE 'satisfied-by-floor' END
FROM groups g JOIN group_state s USING (group_name) WHERE g.lifecycle != 'orphaned'
"""
_START_DELAY_SECS = 5.0
_SUPERVISOR_POLL_INTERVAL_SECS = 1.0
_TOPOLOGIES = {
    'single': (('site', tuple(f'g{index:02d}' for index in range(13))),),
    'dedicated': (
        ('site', tuple(f'g{index:02d}' for index in range(10))),
        ('g10', ('g10',)),
        ('g11', ('g11',)),
        ('g12', ('g12',)),
    ),
    'named': (
        ('site', tuple(f'g{index:02d}' for index in range(10))),
        ('named', ('g10', 'g11', 'g12')),
    ),
}

_SQL_CAPACITY_RESTORE_INSERT_LATE_IDS = (
    'INSERT INTO late_ids (group_name, obligation_id, event_id, terminal_state, '
    "capacity_revision) SELECT :group, obligation_id, event_id, 'done', NULL "
    'FROM source_late_ids WHERE obligation_id BETWEEN :first AND :last'
)

_SQL_CAPACITY_RESTORE_INSERT_RANGE_GROUPS = (
    'INSERT INTO range_groups (obligation_id, group_name, group_generation, '
    'delivery_floor, eligibility_point, obligation_state) SELECT obligation_id, :group, '
    ":generation, 0, opened_successful_at + 1.0, 'pending' FROM discovery_ranges "
    'WHERE obligation_id BETWEEN :first AND :last'
)

_SQL_HEALTH_PROJECTION_READ_SELECT_AUDIT = 'SELECT COUNT(*) FROM audit WHERE group_name = :group'

_SQL_HEALTH_PROJECTION_READ_SELECT_RANGE_GROUPS = (
    "SELECT COUNT(*) FROM range_groups WHERE group_name = :group AND obligation_state = 'pending'"
)

_SQL_INSERT_RANGE = """
INSERT INTO discovery_ranges (obligation_id, low_id, high_id, opened_successful_at,
 visibility_deadline, source_revision, close_generation, range_state, scan_started_at,
 scan_next_id, scan_pages, degraded, scan_cycle_pages)
VALUES (:obligation, :low, :high, :now, :deadline, 1, 0, 'open', NULL, :low, 0, 0, 0)
"""

_SQL_LANE_CURSOR_DELETE_CALLBACK_MARKS = (
    'DELETE FROM callback_marks WHERE group_name = :group AND event_id = :event'
)

_SQL_LANE_CURSOR_SELECT_RANGE_GROUPS = (
    'SELECT r.obligation_id, d.low_id, d.high_id FROM range_groups r '
    'JOIN discovery_ranges d USING (obligation_id) '
    'WHERE r.group_name = :group AND r.group_generation = :generation '
    "AND r.obligation_state = 'pending' "
    'ORDER BY d.low_id, d.high_id, r.obligation_id LIMIT 1'
)

_SQL_LANE_CURSOR_UPDATE_GROUP_STATE = (
    'UPDATE group_state SET cursor = :event, last_delivered_id = :event, '
    'in_flight_event_id = NULL, revision = revision + 1 '
    'WHERE group_name = :group AND revision = :revision'
)

_SQL_LANE_LATE_MARK_INSERT_LATE_IDS = (
    'INSERT INTO late_ids (group_name, obligation_id, event_id, terminal_state, '
    "capacity_revision) VALUES (:group, :obligation, :event, 'done', NULL) "
    'ON CONFLICT (group_name, obligation_id, event_id) '
    "DO UPDATE SET terminal_state = 'done'"
)

_SQL_LANE_LATE_MARK_UPDATE_GROUP_STATE = (
    'UPDATE group_state SET revision = revision + 1 '
    'WHERE group_name = :group AND revision = :revision'
)

_SQL_LANE_MARK_INSERT_CALLBACK_MARKS = (
    'INSERT INTO callback_marks (group_name, event_id, ordinal, plugin_id, callback_id, '
    "outcome, delivery_attempt) VALUES (:group, :event, 0, 'bench-plugin', "
    "'bench-callback', 'done', 1)"
)

_SQL_LANE_MARK_UPDATE_GROUP_STATE = (
    'UPDATE group_state SET in_flight_event_id = :event, revision = revision + 1 '
    'WHERE group_name = :group AND revision = :revision'
)

_SQL_LANE_REFRESH_SELECT_RANGE_GROUPS = (
    'SELECT r.obligation_id, d.low_id, d.high_id, d.range_state, '
    'd.source_revision, d.close_generation '
    'FROM range_groups r JOIN discovery_ranges d USING (obligation_id) '
    'WHERE r.group_name = :group AND r.group_generation = :generation '
    "AND r.obligation_state = 'pending'"
)

_SQL_LANE_REFRESH_SELECT_SOURCE_LATE_IDS = (
    'SELECT event_id FROM source_late_ids WHERE obligation_id = :obligation ORDER BY event_id'
)

_SQL_LANE_REFRESH_SELECT_SOURCE_META = 'SELECT range_revision FROM source_meta WHERE singleton = 1'

_SQL_LANE_SCAN_COMPLETE_DELETE_LATE_IDS = (
    'DELETE FROM late_ids WHERE group_name = :group AND obligation_id = :obligation'
)

_SQL_LANE_SCAN_COMPLETE_UPDATE_RANGE_GROUPS = (
    "UPDATE range_groups SET obligation_state = 'scan-complete' "
    'WHERE obligation_id = :obligation AND group_name = :group '
    "AND group_generation = :generation AND obligation_state = 'pending'"
)

_SQL_LIFECYCLE_REPLAY_DELETE_CALLBACK_MARKS = (
    'DELETE FROM callback_marks WHERE group_name = :group'
)

_SQL_LIFECYCLE_REPLAY_DELETE_EPOCHS = 'DELETE FROM epochs WHERE group_name = :group'

_SQL_LIFECYCLE_REPLAY_DELETE_JOIN_COMMITS = 'DELETE FROM join_commits WHERE group_name = :group'

_SQL_LIFECYCLE_REPLAY_DELETE_LATE_IDS = 'DELETE FROM late_ids WHERE group_name = :group'

_SQL_LIFECYCLE_REPLAY_DELETE_RANGE_GROUPS = (
    'DELETE FROM range_groups WHERE group_name = :group AND group_generation < :generation'
)

_SQL_LIFECYCLE_REPLAY_DELETE_REPLAY_MARKERS = (
    'DELETE FROM replay_markers WHERE group_name = :group'
)

_SQL_LIFECYCLE_REPLAY_INSERT_AUDIT = (
    'INSERT INTO audit (site, group_name, command, wall_clock_time, prior_revision, '
    'new_revision, reason, local_uid, username, pid, disposition_id, command_fingerprint, '
    "terminal_result, result_snapshot_json) VALUES ('bench', :group, 'replay', :wall, "
    ":prior, :new, 'storage gate', :uid, 'bench', :pid, :disposition, :fingerprint, "
    "'source-cleaned', :snapshot)"
)

_SQL_LIFECYCLE_REPLAY_INSERT_DISPOSITION_AUDIT = (
    'INSERT INTO disposition_audit (disposition_id, group_name, command, '
    'command_fingerprint, old_generation, new_generation, cursor, frontier, '
    "stream_incarnation, terminal_result) VALUES (:disposition, :group, 'replay', "
    ":fingerprint, :old, :generation, :floor, :frontier, 'bench-incarnation', "
    "'source-cleaned')"
)

_SQL_SELECT_GROUP_GENERATION = 'SELECT group_generation FROM groups WHERE group_name = :group'

_SQL_LIFECYCLE_REPLAY_SELECT_GROUP_STATE = (
    'SELECT revision, cursor FROM group_state WHERE group_name = :group'
)

_SQL_LIFECYCLE_REPLAY_SELECT_REPLAY_MARKERS = (
    'SELECT from_event_id FROM replay_markers WHERE group_name = :group'
)

_SQL_SELECT_FRONTIER = 'SELECT frontier FROM source_meta WHERE singleton = 1'

_SQL_LIFECYCLE_REPLAY_UPDATE_GROUPS = (
    'UPDATE groups SET group_generation = group_generation + 1 WHERE group_name = :group'
)

_SQL_LIFECYCLE_REPLAY_UPDATE_GROUP_STATE = (
    'UPDATE group_state SET cursor = :floor, delivery_floor = :floor, '
    'last_delivered_id = :floor, in_flight_event_id = NULL, '
    'in_flight_obligation_id = NULL, replay_required_from = NULL, '
    'revision = revision + 1 WHERE group_name = :group'
)

_SQL_POLLER_CLOSE_INSERT_SOURCE_LATE_IDS = (
    'INSERT OR IGNORE INTO source_late_ids (obligation_id, event_id) VALUES (?, ?)'
)

_SQL_POLLER_CLOSE_UPDATE_DISCOVERY_RANGES = (
    "UPDATE discovery_ranges SET range_state = 'closed', "
    'close_generation = close_generation + 1, source_revision = source_revision + 1 '
    'WHERE obligation_id = :obligation'
)

_SQL_POLLER_MERGE_UPDATE_DISCOVERY_RANGES = (
    'UPDATE discovery_ranges SET high_id = :high, source_revision = source_revision + 1 '
    'WHERE obligation_id = :obligation'
)

_SQL_POLLER_MERGE_UPDATE_RANGE_GROUPS = (
    'UPDATE range_groups SET eligibility_point = MAX(eligibility_point, :now + 1.0), '
    "obligation_state = CASE WHEN delivery_floor < :high THEN 'pending' "
    'ELSE obligation_state END WHERE obligation_id = :obligation '
    "AND obligation_state != 'scan-complete'"
)

_SQL_SELECT_INDEX_REVISION = 'SELECT index_revision FROM source_meta WHERE singleton = 1'

_SQL_POLLER_OBSERVE_UPDATE_SOURCE_META = (
    'UPDATE source_meta SET frontier = :frontier, successful_clock = :now WHERE singleton = 1'
)

_SQL_POLLER_RELOAD_SELECT_DISCOVERY_RANGES = (
    'SELECT obligation_id, low_id, high_id, opened_successful_at, '
    'visibility_deadline, range_state FROM discovery_ranges'
)

_SQL_POLLER_RELOAD_SELECT_GROUPS = (
    'SELECT g.group_name, g.group_generation, g.lifecycle, s.delivery_floor '
    'FROM groups g JOIN group_state s USING (group_name)'
)

_SQL_POLLER_RELOAD_SELECT_RANGE_GROUPS = (
    'SELECT obligation_id, group_name, group_generation, obligation_state FROM range_groups'
)

_SQL_POLLER_RELOAD_SELECT_REPLAY_MARKERS = (
    'SELECT group_name, group_generation FROM replay_markers'
)

_SQL_POPULATION_COUNTS_SELECT_DISCOVERY_RANGES = (
    "SELECT COUNT(*) FROM discovery_ranges WHERE range_state = 'open'"
)

_SQL_POPULATION_COUNTS_SELECT_GROUPS = 'SELECT group_name FROM groups'

_SQL_SELECT_PENDING_RANGE_GROUP_COUNTS = (
    "SELECT group_name, COUNT(*) FROM range_groups WHERE obligation_state = 'pending' "
    'GROUP BY group_name'
)

_SQL_READ_TRANSACTION_BEGIN_TRANSACTION = 'BEGIN'

_SQL_REBIND = """
INSERT INTO range_groups (obligation_id, group_name, group_generation, delivery_floor,
 eligibility_point, obligation_state)
SELECT obligation_id, :group, :generation, :floor, opened_successful_at + 1.0,
 CASE WHEN high_id > :floor THEN 'pending' ELSE 'satisfied-by-floor' END
FROM discovery_ranges WHERE range_state = 'open'
"""

_SQL_RETIRE = """
DELETE FROM discovery_ranges WHERE NOT EXISTS (
 SELECT 1 FROM range_groups r WHERE r.obligation_id = discovery_ranges.obligation_id
 AND r.obligation_state = 'pending')
RETURNING obligation_id
"""

_SQL_REVISION_SELECT_GROUP_STATE = 'SELECT revision FROM group_state WHERE group_name = :group'

_SQL_SEED_BASE_INSERT_GROUPS = (
    'INSERT INTO groups (group_name, group_generation, ordering_mode, site_server_url, '
    'verified_incarnation, holdback_secs, active, lifecycle) '
    "VALUES (?, 1, ?, 'https://bench.invalid', 'bench-incarnation', 1.0, 1, 'active')"
)

_SQL_SEED_BASE_INSERT_GROUP_STATE = (
    'INSERT INTO group_state (group_name, revision, cursor, delivery_floor, '
    'last_delivered_id) '
    'VALUES (?, 0, 0, 0, 0)'
)

_SQL_SEED_BASE_INSERT_SOURCE_META = (
    'INSERT INTO source_meta (singleton, site, source_name, stream_fingerprint, '
    'stream_incarnation, source_generation, initialized, frontier, successful_clock) '
    "VALUES (1, 'bench', 'source', 'bench-stream', 'bench-incarnation', "
    "'bench-generation', 1, 0, 0.0)"
)

_SQL_SEED_AUDIT_INSERT_AUDIT = (
    'INSERT INTO audit (site, group_name, command, wall_clock_time, '
    'prior_revision, new_revision, reason, local_uid, username, pid) '
    "VALUES ('bench', ?, 'replay', 0.0, 0, 1, 'seed', ?, 'bench', ?)"
)

_SQL_BEGIN_IMMEDIATE = 'BEGIN IMMEDIATE'

_SQL_SEED_CAP_STRESS_INSERT_GROUPS = (
    'INSERT INTO groups (group_name, group_generation, ordering_mode, '
    'site_server_url, '
    'verified_incarnation, holdback_secs, active, lifecycle) '
    "VALUES ('orphan', 1, 'eager', 'https://bench.invalid', "
    "'bench-incarnation', 1.0, 0, 'orphaned')"
)

_SQL_SEED_CAP_STRESS_INSERT_GROUP_STATE = (
    'INSERT INTO group_state (group_name, revision, cursor, delivery_floor, '
    'last_delivered_id, orphaned, parked, replay_required_from) '
    "VALUES ('orphan', 0, 0, 0, 0, 1, 1, 1000)"
)

_SQL_SEED_CAP_STRESS_INSERT_REPLAY_MARKERS = (
    'INSERT INTO replay_markers (group_name, group_generation, source_generation, '
    'marker_id, from_event_id, command_digest) '
    "VALUES ('orphan', 1, 'bench-generation', 'seed-marker', 1000, 'seed-digest')"
)

_SQL_WAL_CHECKPOINT_TRUNCATE = 'PRAGMA wal_checkpoint(TRUNCATE)'

_SQL_SEED_CAP_STRESS_UPDATE_SOURCE_META = 'UPDATE source_meta SET frontier = 1000000'

_SQL_SEED_CLOSED_RANGES_INSERT_LATE_IDS = (
    'INSERT INTO late_ids (group_name, obligation_id, event_id, terminal_state, '
    "capacity_revision) VALUES (?, ?, ?, 'done', NULL)"
)

_SQL_SEED_CLOSED_RANGES_INSERT_RANGE_GROUPS = (
    'INSERT INTO range_groups (obligation_id, group_name, group_generation, '
    'delivery_floor, eligibility_point, obligation_state) VALUES (?, ?, 1, 0, 0.0, ?)'
)

_SQL_SEED_CLOSED_RANGES_INSERT_SOURCE_LATE_IDS = (
    'INSERT INTO source_late_ids (obligation_id, event_id) VALUES (?, ?)'
)

_SQL_SEED_CLOSED_RANGES_UPDATE_DISCOVERY_RANGES = (
    "UPDATE discovery_ranges SET range_state = 'closed' WHERE obligation_id = ?"
)

_SQL_SNAPSHOT_READ_SELECT_CALLBACK_MARKS = 'SELECT * FROM callback_marks WHERE group_name = :group'

_SQL_SNAPSHOT_READ_SELECT_EPOCHS = 'SELECT * FROM epochs WHERE group_name = :group'

_SQL_SNAPSHOT_READ_SELECT_GROUPS = 'SELECT * FROM groups WHERE group_name = :group'

_SQL_SNAPSHOT_READ_SELECT_GROUP_STATE = 'SELECT * FROM group_state WHERE group_name = :group'

_SQL_SNAPSHOT_READ_SELECT_LATE_IDS = (
    "SELECT COUNT(*) FROM late_ids WHERE group_name = :group AND terminal_state != 'done'"
)

_SQL_STATUS_READ_SELECT_AUDIT = 'SELECT group_name, COUNT(*) FROM audit GROUP BY group_name'

_SQL_STATUS_READ_SELECT_DISCOVERY_RANGES = (
    'SELECT range_state, COUNT(*) FROM discovery_ranges GROUP BY range_state'
)

_SQL_STATUS_READ_SELECT_SOURCE_META = 'SELECT * FROM source_meta'

_SQL_STRICT_EPOCH_DELETE_EPOCHS = (
    "DELETE FROM epochs WHERE group_name = :group AND state = 'complete' "
    'AND epoch_id < (SELECT MAX(epoch_id) - 8 FROM epochs WHERE group_name = :group)'
)

_SQL_STRICT_EPOCH_INSERT_EPOCHS = (
    'INSERT INTO epochs (group_name, low_id, high_id, captured_at, eligibility_point, '
    'reader_clock_checkpoint, source_generation, group_generation, state) '
    "VALUES (:group, :low, :high, :now, :now, :now, 'bench-generation', "
    ":generation, 'pending')"
)

_SQL_STRICT_EPOCH_UPDATE_EPOCHS = (
    "UPDATE epochs SET state = 'complete' WHERE epoch_id = "
    '(SELECT MIN(epoch_id) FROM epochs '
    "WHERE group_name = :group AND state = 'pending')"
)

_SQL_STRICT_EPOCH_UPDATE_GROUP_STATE = (
    'UPDATE group_state SET last_epoch_capture = :now, revision = revision + 1 '
    'WHERE group_name = :group'
)

_SQL_STRICT_READER_CLOCK_UPDATE_GROUP_STATE = (
    'UPDATE group_state SET reader_clock_checkpoint = :now, revision = revision + 1 '
    'WHERE group_name = :group'
)

_STRICT_CLOCK_EVERY = 25
_STRICT_EPOCH_EVERY = 10
_STRICT_GROUPS = _GROUP_NAMES[:3]


class _IndexChangedError(Exception):
    """Signal that a poller's cached range index needs reloading."""


class _LaneChangedError(Exception):
    """Reject prepared lane state that changed before protected transaction admission."""


class _InvalidResultError(ValueError):
    """Report invalid external JSON values at the result-file boundary."""


@dataclass(frozen=True)
class _RangeSource:
    """Keep immutable source evidence apart from mutable lane scan progress."""

    version: tuple[int, int, str, int, int, int]
    late_ids: tuple[int, ...]


@dataclass
class _ScanWork:
    """Track the late ids remaining before one scan completion."""

    obligation_id: str
    late_ids: list[int]


@dataclass
class _LaneState:
    """Keep one group's in-memory delivery and scan state."""

    group_name: str
    generation: int = 1
    strict: bool = False
    revision_seen: int = -1
    next_event_id: int = 1
    arrivals: int = 0
    in_flight: int | None = None
    scan_work: list[_ScanWork] = field(default_factory=list[_ScanWork])
    scan_suppressed: frozenset[str] = frozenset()
    epoch_turn: int = 0
    next_snapshot_at: float = 0.0
    next_coordination_at: float = 0.0
    health_late_ids: tuple[int, list[int]] | None = None
    source_ranges: dict[str, _RangeSource] = field(default_factory=dict[str, _RangeSource])


@dataclass
class _PollerState:
    """Keep the source index seen by one poller."""

    frontier: int = 0
    index_revision: int = 0
    cycle: int = 0
    open_ranges: list[tuple[str, int, int, float]] = field(
        default_factory=list[tuple[str, int, int, float]]
    )
    next_range: int = 0
    hold_open: bool = False
    seed: str = 'bench'
    poller_rate: float = 2.0


@dataclass
class _PollPlan:
    """Describe one poller's decisions before it takes the write lock."""

    frontier: int
    opened: list[tuple[str, int, int, float]] = field(
        default_factory=list[tuple[str, int, int, float]]
    )
    merged: tuple[str, int] | None = None
    closed: list[tuple[str, list[int]]] = field(default_factory=list[tuple[str, list[int]]])


@dataclass(frozen=True)
class _Scenario:
    """Define aligned lane rates and optional replay or burst traffic."""

    lane_rate: float
    lifecycle: bool = False
    catchup: bool = False
    poller_rate: float = 2.0

    def segments(self, lane: str, duration: float) -> list[tuple[float, float, float]]:
        """Return half-open (start, end, rate) segments in seconds for one lane."""
        if not self.catchup or lane not in _CATCHUP_GROUPS:
            return [(0.0, duration, self.lane_rate)]
        segments: list[tuple[float, float, float]] = []
        offset = 0.0
        burst = 0.0
        fast = True
        while offset < duration:
            end = min(duration, offset + _BURST_SECS)
            rate = 310.0 if fast and burst < _BURST_TOTAL_SECS else 10.0
            segments.append((offset, end, rate))
            if fast:
                burst += end - offset
            offset = end
            fast = not fast
        return segments


@dataclass(frozen=True)
class _Cell:
    """Identify one scenario, topology, durability, and population combination."""

    number: int
    scenario: str
    topology: str
    synchronous: str
    arbitration: str
    population: str
    gated: bool


@dataclass(frozen=True)
class _RunOptions:
    """Carry cell paths and measurement overrides for the command layer."""

    work_dir: Path
    duration_secs: float | None = None
    min_samples: int = _MIN_SAMPLES
    checkpoint_interval_secs: float = DEFAULT_CHECKPOINT_INTERVAL_SECS
    repetition: int = 1
    device_class: str = 'unspecified'
    filesystem: str = 'unspecified'
    commit: str | None = None
    seed: str = 'bench'
    extra_timing: bool | None = None
    diagnostic_cap: int = _DIAGNOSTIC_CAP


@dataclass(frozen=True)
class _ProcessSpec:
    """Serialize one process's workload and shared monotonic schedule."""

    role: str
    database_path: str
    lanes: tuple[str, ...]
    poller: bool
    lifecycle: bool
    status_reader: bool
    scenario: str
    synchronous: str
    arbitration: str
    population: str
    start_at: float
    end_at: float
    harness_clock_start: float
    checkpoint_interval_secs: float
    seed: str
    scan_suppressed: tuple[str, ...]
    output_path: str
    extra_timing: bool = False
    diagnostic_cap: int = _DIAGNOSTIC_CAP

    @classmethod
    def from_json(cls, value: str) -> _ProcessSpec:
        """Decode the internal JSON process specification.

        Args:
            value: JSON object containing the process fields emitted by
                `to_json`; `lanes` and `scan_suppressed` arrays become tuples.

        Raises:
            ValueError: The process specification is not valid JSON.
            KeyError: A required collection field is absent.
            TypeError: The decoded fields cannot construct the process
                specification.
        """
        data = json.loads(value)
        data['lanes'] = tuple(data['lanes'])
        data['scan_suppressed'] = tuple(data['scan_suppressed'])
        return cls(**data)

    def to_json(self) -> str:
        """Encode all process fields for a child interpreter."""
        return json.dumps(asdict(self))


@dataclass
class _LifecycleState:
    """Track round-robin replays without consuming a turn for capacity replay."""

    position: int = 0
    capacity_due: bool = False


class _BusyOnlyLock:
    """Serialize local writers while leaving cross-process arbitration to SQLite."""

    def __init__(self, *, timeout_secs: float) -> None:
        """Set the acquisition budget and create the process-local mutex."""
        self._timeout_secs = timeout_secs
        self._local = threading.Lock()

    def close(self) -> None:
        """Release no external resources."""

    @contextmanager
    def transaction(
        self,
        connection: sqlite3.Connection,
        *,
        timing: WriteTiming,
        budget_secs: float | None = None,
        stop: threading.Event | None = None,
    ) -> Generator[sqlite3.Connection]:
        """Run an immediate transaction with a capped local and SQLite wait.

        Args:
            connection: The calling thread's autocommit connection.
            timing: Mutable transaction phase record.
            budget_secs: Optional acquisition budget override in seconds.
            stop: Optional signal to abandon acquisition.

        Yields:
            The connection inside its immediate transaction.

        Raises:
            WriteLockStoppedError: Acquisition is abandoned after a stop
                signal.
            WriteLockTimeoutError: The local lock wait or an acquisition-budget
                check exhausts the allowed time.
            DatabaseError: SQLite transaction setup, completion, rollback, or
                busy-timeout restoration fails.
        """
        timing.started_at = time.monotonic()
        budget = self._timeout_secs if budget_secs is None else budget_secs
        deadline = timing.started_at + budget
        remaining = _busy_remaining(deadline, stop, 'process-local')
        if not self._local.acquire(timeout=remaining):
            raise WriteLockTimeoutError('busy-only local wait expired', 'process-local')
        try:
            timing.local_acquired_at = time.monotonic()
            timing.file_acquired_at = timing.local_acquired_at
            _busy_remaining(deadline, stop, 'process-local')
            _busy_begin(connection, deadline, stop, timing)
            yield connection
            connection.commit()
            timing.finished_at = time.monotonic()
        except sqlite3.Error as exc:
            raise DatabaseError(str(exc)) from exc
        finally:
            try:
                _busy_settle(connection, timing)
            finally:
                self._local.release()


@dataclass
class _CapacityState:
    """Share replay readiness and restore requests within the site process."""

    ready: threading.Event = field(default_factory=threading.Event)
    restore: threading.Event = field(default_factory=threading.Event)
    counts: dict[str, int] = field(default_factory=dict[str, int])


class _PhaseRecords:
    """Keep bounded packed call spans without retaining per-call Python graphs."""

    def __init__(self, capacity: int) -> None:
        """Preallocate all bytes before measurement; zero capacity disables recording."""
        self.capacity = capacity
        self.overflow = 0
        self._data = bytearray(capacity * _PHASE_ROW.size)
        self._count = 0
        self._identity = 0
        self._lock = threading.Lock()

    def identity(self) -> int:
        """Allocate a process-local correlation ID for one attempt or checkpoint."""
        with self._lock:
            self._identity += 1
            return self._identity

    def records(self) -> Iterator[dict[str, Any]]:
        """Decode one phase at a time after measured owners have joined.

        Yields:
            Records in reserved-slot order with `phase_id`, `thread_id`,
            `phase`, `outcome`, `start`, and `end`. Each clock mapping contains
            `monotonic`, `thread_cpu`, and `process_cpu` seconds; outcome is
            `completed` or `failed`.
        """
        for index in range(self._count):
            identity, thread_id, kind, completed, *clocks = _PHASE_ROW.unpack_from(
                self._data, index * _PHASE_ROW.size
            )
            yield {
                'phase_id': identity,
                'thread_id': thread_id,
                'phase': _PHASE_NAMES[kind],
                'outcome': 'completed' if completed else 'failed',
                'start': dict(
                    zip(('monotonic', 'thread_cpu', 'process_cpu'), clocks[:3], strict=True)
                ),
                'end': dict(
                    zip(('monotonic', 'thread_cpu', 'process_cpu'), clocks[3:], strict=True)
                ),
            }

    @contextmanager
    def span(self, phase: str, identity: int) -> Generator[None]:
        """Sample one call, preserving its exception and counting every dropped span.

        Args:
            phase: One of `begin`, `body`, `commit`, `passive`, `truncate`, or
                `sync`.
            identity: Unsigned 64-bit correlation ID shared by related phases.

        Yields:
            Control while the call runs. Disabled or full storage still runs
            the call; full storage increments the dropped-span count.
        """
        if not self.capacity:
            yield
            return
        with self._lock:
            if self._count == self.capacity:
                self.overflow += 1
                index = -1
            else:
                index = self._count
                self._count += 1
        if index < 0:
            yield
            return
        thread_id = threading.get_ident()
        kind = _PHASE_NAMES.index(phase)
        wall, thread, process = time.monotonic(), time.thread_time(), time.process_time()
        completed = False
        try:
            yield
            completed = True
        finally:
            end_wall, end_thread, end_process = (
                time.monotonic(),
                time.thread_time(),
                time.process_time(),
            )
            _PHASE_ROW.pack_into(
                self._data,
                index * _PHASE_ROW.size,
                identity,
                thread_id,
                kind,
                completed,
                wall,
                thread,
                process,
                end_wall,
                end_thread,
                end_process,
            )


class _ConnectionObserver:
    """Delegate the lock's connection calls, observing only BEGIN and COMMIT."""

    def __init__(
        self, connection: sqlite3.Connection, phases: _PhaseRecords, identity: int
    ) -> None:
        """Bind the existing connection to one writer attempt's phase identity."""
        self._connection = connection
        self._phases = phases
        self._identity = identity

    def __getattr__(self, name: str) -> object:
        """Forward transaction state, rollback and other unchanged connection behavior."""
        return getattr(self._connection, name)

    def commit(self) -> None:
        """Commit exactly once, retaining a failed-call return boundary on exceptions.

        Raises:
            sqlite3.Error: SQLite cannot commit the transaction.
        """
        with self._phases.span('commit', self._identity):
            self._connection.commit()

    def execute(self, sql: str) -> sqlite3.Cursor:
        """Execute exactly once, enclosing each BEGIN retry in its own span.

        Returns:
            The underlying connection's cursor for the statement.

        Raises:
            sqlite3.Error: SQLite cannot execute the statement.
        """
        if sql == _SQL_BEGIN_IMMEDIATE:
            with self._phases.span('begin', self._identity):
                return self._connection.execute(sql)
        return self._connection.execute(sql)


class _PackedRecords(Sequence[dict[str, Any]]):
    """Retain append-only record snapshots without per-record Python object graphs."""

    def __init__(self) -> None:
        """Allocate an empty byte buffer and unsigned record boundaries."""
        self._data = bytearray()
        self._offsets = array('Q', [0])
        self._overflowed = False
        self._lock = threading.RLock()

    @overload
    def __getitem__(self, index: int) -> dict[str, Any]: ...

    @overload
    def __getitem__(self, index: slice) -> list[dict[str, Any]]: ...

    def __getitem__(self, index: int | slice) -> dict[str, Any] | list[dict[str, Any]]:
        """Decode one snapshot or a slice, preserving ordinary sequence indexing."""
        with self._lock:
            if isinstance(index, slice):
                return [self[position] for position in range(*index.indices(len(self)))]
            count = len(self)
            if index < 0:
                index += count
            if index < 0 or index >= count:
                raise IndexError('packed record index out of range')
            return cast(
                'dict[str, Any]',
                json.loads(self._data[self._offsets[index] : self._offsets[index + 1]]),
            )

    def __len__(self) -> int:
        """Return the number of complete retained snapshots."""
        return len(self._offsets) - 1

    def _append(self, record: dict[str, Any], cap: int | None) -> bool:
        """Encode a snapshot, then admit and append it without tracked allocations."""
        encoded = json.dumps(record, separators=(',', ':')).encode()
        with self._lock:
            if cap is not None and len(self) >= cap:
                return False
            # Validate the new boundary before changing the retained byte buffer.
            try:
                self._offsets.append(len(self._data) + len(encoded))
            except OverflowError:
                self._overflowed = True
                raise
            try:
                self._data.extend(encoded)
            except BaseException:
                self._overflowed = True
                self._offsets.pop()
                raise
            return True

    def append(self, record: dict[str, Any]) -> None:
        """Snapshot a record, raising rather than dropping evidence on offset overflow.

        Args:
            record: Trusted harness values to retain with their JSON values and
                field order.

        Raises:
            OverflowError: The buffer end cannot fit the unsigned offset
                representation.
        """
        self._append(record, None)

    def append_if_room(self, record: dict[str, Any], cap: int) -> bool:
        """Encode before admission, rejecting a record if reentrant work filled the cap.

        Args:
            record: Trusted harness snapshot to retain.
            cap: The existing per-process diagnostic record capacity.

        Returns:
            Whether the complete snapshot was retained.

        Raises:
            OverflowError: The buffer end cannot fit the unsigned offset
                representation.
        """
        return self._append(record, cap)

    def records(self) -> Iterator[dict[str, Any]]:
        """Decode snapshots incrementally, refusing prior offset overflow.

        Raises:
            OverflowError: A previous append left incomplete evidence.
        """
        if self._overflowed:
            raise OverflowError('packed record offsets overflowed; evidence is incomplete')
        yield from self


@dataclass
class _Runtime:
    """Collect process-local records from independent connection-owning threads."""

    spec: _ProcessSpec
    lock: SourceWriteLock | _BusyOnlyLock
    stop: threading.Event = field(default_factory=threading.Event)
    go: threading.Event = field(default_factory=threading.Event)
    capacity: _CapacityState = field(default_factory=_CapacityState)
    commits: _PackedRecords = field(default_factory=_PackedRecords)
    reads: _PackedRecords = field(default_factory=_PackedRecords)
    checkpoints: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    samples: list[tuple[int, int]] = field(default_factory=list[tuple[int, int]])
    errors: list[str] = field(default_factory=list[str])
    expired_attempts: list[str] = field(default_factory=list[str])
    capacity_replays: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    busy_timeouts: int = 0
    index_reloads: int = 0
    restore_commits: int = 0
    checkpoint_halt: str | None = None
    checkpoint_crossings: int = 0
    checkpoint_started_at: float | None = None
    checkpoint_finished_at: float | None = None
    finished_at: float = 0.0
    wal_trigger_bytes: int = WAL_TRIGGER_BYTES
    diagnostics: _PackedRecords = field(default_factory=_PackedRecords)
    diagnostic_overflow: int = 0
    diagnostic_lock: threading.RLock = field(default_factory=threading.RLock)
    phases: _PhaseRecords = field(init=False)
    writer_attempts: dict[str, tuple[float, int]] = field(
        default_factory=dict[str, tuple[float, int]]
    )

    def __post_init__(self) -> None:
        """Allocate phase capacity before any measured thread is started."""
        self.phases = _PhaseRecords(self.spec.diagnostic_cap if self.spec.extra_timing else 0)


@dataclass(frozen=True)
class _Writer:
    """Bind one immutable arrival schedule to its model operation and reads."""

    name: str
    arrivals: tuple[float, ...]
    perform: Callable[[sqlite3.Connection, float], str]
    before: Callable[[sqlite3.Connection, float], object] | None = None
    after: Callable[[sqlite3.Connection], None] | None = None
    reload: Callable[[sqlite3.Connection], None] | None = None
    maintenance: bool = False
    wait: Callable[[float], bool] | None = None


@dataclass(frozen=True)
class _ThreadJob:
    """Give one thread its connection policy, startup signal, and workload."""

    name: str
    run: Callable[[_Runtime, sqlite3.Connection], None]
    synchronous: str
    ready: threading.Event = field(default_factory=threading.Event)


@dataclass(frozen=True)
class _FailureBoundary:
    """Contain a thread or setup failure after publishing it and stopping the process."""

    runtime: _Runtime
    name: str

    def __enter__(self) -> None:
        """Enter the process failure boundary."""

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        _traceback: TracebackType | None,
    ) -> bool:
        """Publish any escaping failure and prevent silent background exceptions."""
        if error is not None and kind is not None:
            self.runtime.errors.append(f'{self.name}: {kind.__name__}: {error}')
            self.runtime.stop.set()
        return True


@dataclass(frozen=True)
class _CellRun:
    """Keep a cell's metadata and common schedule together for aggregation."""

    cell: _Cell
    options: _RunOptions
    start_at: float
    end_at: float
    population: dict[str, Any]
    directory: Path | None = None


def _arrival_accounting(records: list[dict[str, Any]], end: float) -> dict[str, Any]:
    """Reconcile unique offered IDs against completions at each shared boundary."""
    offered = {item['arrival_id'] for raw in records for item in raw['offered_arrivals']}
    commits = [item for raw in records for item in raw['commits'] if not item.get('maintenance')]
    accounting: dict[str, Any] = {}
    for name, boundary in [('load_end', end), ('drain_end', end + _DRAIN_SECS)]:
        timely = {item['arrival_id'] for item in commits if item['finished_at'] <= boundary}
        outstanding = sorted(offered - timely)
        accounting[name] = {
            'offered': len(offered),
            'completed': len(timely),
            'outstanding': len(outstanding),
            'outstanding_ids': outstanding,
        }
    return accounting


def _arrival_times(
    segments: list[tuple[float, float, float]], start_at: float, end_at: float
) -> Iterator[float]:
    """Generate immutable scheduled times without consulting completion time."""
    for first, last, rate in segments:
        count = math.ceil((min(last, end_at - start_at) - first) * rate)
        for index in range(max(0, count)):
            arrival = start_at + first + index / rate
            if start_at <= arrival < end_at:
                yield arrival


def _busy_remaining(deadline: float, stop: threading.Event | None, level: WriteLockLevel) -> float:
    """Reject stopped or expired acquisition at the current admission boundary."""
    if stop is not None and stop.is_set():
        raise WriteLockStoppedError('busy-only acquisition stopped')
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise WriteLockTimeoutError(f'busy-only {level} wait expired', level)
    return remaining


def _busy_restore_timeout(connection: sqlite3.Connection, configured: int) -> None:
    """Restore SQLite's timeout, promoting restoration failures over acquisition expiry."""
    primary = sys.exception()
    try:
        connection.execute(f'PRAGMA busy_timeout={configured}')
    except sqlite3.Error as exc:
        if primary is None or isinstance(primary, WriteLockTimeoutError):
            failure = DatabaseError(str(exc))
            if primary is not None:
                failure.add_note(f'write lock acquisition expiry: {primary!r}')
            raise failure from exc
        primary.add_note(f'busy_timeout restore failure: {exc!r}')


def _busy_begin(
    connection: sqlite3.Connection,
    deadline: float,
    stop: threading.Event | None,
    timing: WriteTiming,
) -> None:
    """Temporarily cap SQLite's busy handler at the remaining acquisition budget."""
    configured = int(connection.execute('PRAGMA busy_timeout').fetchone()[0])
    try:
        remaining = _busy_remaining(deadline, stop, 'SQLite')
        connection.execute(f'PRAGMA busy_timeout={max(1, math.floor(remaining * 1000))}')
        _busy_remaining(deadline, stop, 'SQLite')
        connection.execute(_SQL_BEGIN_IMMEDIATE)
        timing.begun_at = time.monotonic()
    finally:
        _busy_restore_timeout(connection, configured)


def _busy_settle(connection: sqlite3.Connection, timing: WriteTiming) -> None:
    """Time rollback completion without replacing an error already in flight."""
    if not connection.in_transaction:
        return
    primary = sys.exception()
    try:
        connection.rollback()
    except sqlite3.Error as exc:
        if primary is None:
            raise DatabaseError(str(exc)) from exc
        primary.add_note(f'rollback failure: {exc!r}')
    finally:
        timing.finished_at = time.monotonic()


def _capacity_chunks() -> list[tuple[str, str]]:
    """Partition the seeded closed ranges into fifty restore transactions."""
    return [
        (f'seed-closed-{first:03d}', f'seed-closed-{first + _RESTORE_CHUNK_RANGES - 1:03d}')
        for first in range(0, 900, _RESTORE_CHUNK_RANGES)
    ]


def _capacity_restore(
    connection: sqlite3.Connection, group_name: str, generation: int, first: str, last: str
) -> None:
    """Restore a pinned group's closed-range rows without publishing revisions."""
    parameters = {'group': group_name, 'generation': generation, 'first': first, 'last': last}
    connection.execute(
        _SQL_CAPACITY_RESTORE_INSERT_RANGE_GROUPS,
        parameters,
    )
    connection.execute(
        _SQL_CAPACITY_RESTORE_INSERT_LATE_IDS,
        parameters,
    )


def _capacity_shape_eligible(
    shape: dict[str, Any], replay: dict[str, Any], duration_secs: float
) -> bool:
    """Check the minimum observed capacity and replay shape."""
    return (
        all(
            shape.get(name, {}).get('min', 0) >= minimum
            for name, minimum in _CAPACITY_MINIMUMS.items()
        )
        and bool(replay)
        and (
            replay.get('count', 0) >= int(duration_secs // 60) - 1
            and replay.get('min_late_ids_before', 0) >= _CAPACITY_MINIMUMS['late_ids']
            and replay.get('min_pending_before', 0) >= _CAPACITY_MINIMUMS['pending:g11']
        )
    )


def _cell_specs(run: _CellRun, directory: Path) -> list[_ProcessSpec]:
    """Assign aligned process roles and ownership for the selected topology."""
    cell = run.cell
    suppressed = (
        tuple(f'seed-closed-{index:03d}' for index in range(900))
        if cell.population == 'cap-stress'
        else ()
    )
    return [
        _ProcessSpec(
            role,
            str(directory / 'source.db'),
            lanes,
            role == 'site',
            role == 'site' and cell.scenario == 'lifecycle',
            role == 'site',
            cell.scenario,
            cell.synchronous.upper(),
            cell.arbitration,
            cell.population,
            run.start_at,
            run.end_at,
            float(run.population.get('harness_clock', 0.0)),
            run.options.checkpoint_interval_secs,
            run.options.seed,
            suppressed,
            str(directory / f'{role}-records.json'),
            run.options.extra_timing
            if run.options.extra_timing is not None
            else cell.number == _DIAGNOSTIC_CELL,
            run.options.diagnostic_cap,
        )
        for role, lanes in _TOPOLOGIES[cell.topology]
    ]


def _checkpoint_summary(
    attempts: list[dict[str, Any]], start_at: float, end_at: float, interval: float
) -> dict[str, Any]:
    """Report outcome counts and the longest interval without an observed clear."""
    cleared = start_at
    longest = 0.0
    for attempt in attempts:
        finished = float(attempt['finished_at'])
        longest = max(longest, finished - cleared)
        if attempt['outcome'] in {'complete', 'reset-observed'} or attempt['reuse'] == 'reused':
            cleared = finished
    longest = max(longest, end_at - cleared)
    return {
        'attempts': len(attempts),
        'outcomes': dict(
            Counter(outcome for item in attempts for outcome in (item['outcome'], item['reuse']))
        ),
        'longest_uncleared_secs': longest,
        'longest_uncleared_intervals': longest / interval,
        'peak_log_frames': max(
            (item['result'][1] for item in attempts if item['outcome'] != 'contended'), default=0
        ),
    }


def _clock_sample() -> dict[str, float]:
    """Sample one correlation clock and two separate cumulative CPU clocks."""
    return {
        'monotonic': time.monotonic(),
        'process_cpu': time.process_time(),
        'thread_cpu': time.thread_time(),
    }


def _default_duration(scenario: _Scenario, min_samples: int) -> float:
    """Run long enough for both the time and offered-sample minimums."""
    rate = len(_GROUP_NAMES) * scenario.lane_rate + scenario.poller_rate
    return max(_MIN_DURATION_SECS, min_samples / rate)


def _eligible_for_verdict(result: dict[str, Any]) -> bool:
    """Validate recorded eligibility and cap shape when reading result files."""
    if not result.get('gate_eligible', False):
        return False
    if result['cell']['population'] != 'cap-stress':
        return True
    shape = result.get('population_during_run', {})
    replay: dict[str, Any] = result.get('capacity_replays') or {}
    return _capacity_shape_eligible(shape, replay, result['duration_secs'])


def _evaluation_status(evaluations: list[dict[str, Any]]) -> int:
    """Fail until every selected gated cell passes."""
    return int(
        any(item['qualification_verdict'] not in {'pass', 'reported'} for item in evaluations)
    )


def _lane_resume(connection: sqlite3.Connection, lane: _LaneState) -> None:
    """Resume durable cursor and callback progress from a seed or replay."""
    row = connection.execute(
        'SELECT cursor, in_flight_event_id FROM group_state WHERE group_name = ?',
        (lane.group_name,),
    ).fetchone()
    lane.in_flight = int(row[1]) if row[1] is not None else None
    lane.next_event_id = lane.in_flight if lane.in_flight is not None else int(row[0]) + 1


def _lane_scan_complete(connection: sqlite3.Connection, lane: _LaneState, work: _ScanWork) -> None:
    """Complete a pending range for one group and publish changed revisions."""
    cursor = connection.execute(
        _SQL_LANE_SCAN_COMPLETE_UPDATE_RANGE_GROUPS,
        {
            'obligation': work.obligation_id,
            'group': lane.group_name,
            'generation': lane.generation,
        },
    )
    if cursor.rowcount:
        connection.execute(
            _SQL_LANE_SCAN_COMPLETE_DELETE_LATE_IDS,
            {'group': lane.group_name, 'obligation': work.obligation_id},
        )
        connection.execute(BUMP_INDEX_REVISION)
        connection.execute(BUMP_RANGE_REVISION)


def _lane_source_ranges(
    connection: sqlite3.Connection, lane: _LaneState, generation: int
) -> dict[str, _RangeSource]:
    """Validate cached closed-range evidence against metadata in the current snapshot."""
    rows = connection.execute(
        _SQL_LANE_REFRESH_SELECT_RANGE_GROUPS,
        {'group': lane.group_name, 'generation': generation},
    ).fetchall()
    sources: dict[str, _RangeSource] = {}
    for obligation, low, high, state, source_revision, close_generation in rows:
        version = (low, high, state, source_revision, close_generation, generation)
        previous = lane.source_ranges.get(obligation)
        if state == 'closed' and previous is not None and previous.version == version:
            sources[obligation] = previous
        elif state == 'closed':
            late_ids = connection.execute(
                _SQL_LANE_REFRESH_SELECT_SOURCE_LATE_IDS, {'obligation': obligation}
            ).fetchall()
            sources[obligation] = _RangeSource(version, tuple(row[0] for row in late_ids))
        else:
            sources[obligation] = _RangeSource(version, ())
    return sources


def _lifecycle_replay(connection: sqlite3.Connection, group_name: str, now: float) -> None:
    """Reset a group and rebind its open ranges at a new generation."""
    row = connection.execute(
        _SQL_LIFECYCLE_REPLAY_SELECT_GROUP_STATE,
        {'group': group_name},
    ).fetchone()
    if row is None:
        raise ValueError(f'unknown group: {group_name}')
    prior = int(row[0])
    frontier = int(connection.execute(_SQL_SELECT_FRONTIER).fetchone()[0])
    connection.execute(
        _SQL_LIFECYCLE_REPLAY_SELECT_REPLAY_MARKERS,
        {'group': group_name},
    ).fetchone()
    old = int(
        connection.execute(
            _SQL_SELECT_GROUP_GENERATION,
            {'group': group_name},
        ).fetchone()[0]
    )
    generation = old + 1
    floor = max(0, frontier - _REPLAY_DISTANCE)
    connection.execute(
        _SQL_LIFECYCLE_REPLAY_UPDATE_GROUPS,
        {'group': group_name},
    )
    connection.execute(
        _SQL_LIFECYCLE_REPLAY_UPDATE_GROUP_STATE,
        {'floor': floor, 'group': group_name},
    )
    connection.execute(_SQL_LIFECYCLE_REPLAY_DELETE_CALLBACK_MARKS, {'group': group_name})
    connection.execute(_SQL_LIFECYCLE_REPLAY_DELETE_LATE_IDS, {'group': group_name})
    connection.execute(_SQL_LIFECYCLE_REPLAY_DELETE_EPOCHS, {'group': group_name})
    connection.execute(_SQL_LIFECYCLE_REPLAY_DELETE_JOIN_COMMITS, {'group': group_name})
    connection.execute(
        _SQL_LIFECYCLE_REPLAY_DELETE_RANGE_GROUPS,
        {'group': group_name, 'generation': generation},
    )
    connection.execute(_SQL_LIFECYCLE_REPLAY_DELETE_REPLAY_MARKERS, {'group': group_name})
    connection.execute(
        _SQL_REBIND, {'group': group_name, 'generation': generation, 'floor': floor}
    )
    disposition = f'bench-{group_name}-{generation}'
    fingerprint = f'replay-{group_name}-{generation}'
    connection.execute(
        _SQL_LIFECYCLE_REPLAY_INSERT_AUDIT,
        {
            'group': group_name,
            'wall': now,
            'prior': prior,
            'new': prior + 1,
            'uid': os.getuid(),
            'pid': os.getpid(),
            'disposition': disposition,
            'fingerprint': fingerprint,
            'snapshot': 'x' * _SNAPSHOT_BLOB_BYTES,
        },
    )
    connection.execute(
        _SQL_LIFECYCLE_REPLAY_INSERT_DISPOSITION_AUDIT,
        {
            'disposition': disposition,
            'group': group_name,
            'fingerprint': fingerprint,
            'old': old,
            'generation': generation,
            'floor': floor,
            'frontier': frontier,
        },
    )
    connection.execute(BUMP_INDEX_REVISION)
    connection.execute(BUMP_RANGE_REVISION)


def _lifecycle_target(
    state: _LifecycleState, arrival: int, *, capacity_ready: bool, population: str
) -> str:
    """Select the next replay target, preserving a delayed capacity obligation."""
    if population == 'cap-stress':
        state.capacity_due |= arrival % _CAPACITY_REPLAY_EVERY == _CAPACITY_REPLAY_EVERY - 1
        if state.capacity_due and capacity_ready:
            state.capacity_due = False
            return _CAPACITY_GROUP
    target = _REPLAY_GROUPS[state.position % len(_REPLAY_GROUPS)]
    state.position += 1
    return target


def _limit_checks(result: dict[str, Any]) -> list[tuple[str, float, float]]:
    """Return all measured maxima and their section 9.2 limits."""
    cell = result['cell']
    durability = cell['synchronous'].lower()
    limits = _DURABILITY_LIMITS[durability]
    checks = [('all_commit_latency_ms.max', result['all_commit_latency_ms']['max'], limits['max'])]
    if cell['scenario'] in {'documented', 'worst'}:
        checks.extend(
            (f'commit_latency_ms.{key}', result['commit_latency_ms'][key], limits[key])
            for key in ('p99', 'p999')
        )
    if cell['scenario'] == 'catchup' and durability == 'normal':
        checks.append(
            ('commit_latency_ms.p99', result['commit_latency_ms']['p99'], _LIMITS['catchup_p99'])
        )
    if cell['scenario'] == 'lifecycle':
        checks.append(
            (
                'full_lifecycle_latency_ms.p99',
                result['full_lifecycle_latency_ms']['p99'],
                _LIMITS['lifecycle_p99'],
            )
        )
    checks.append(
        ('peak_wal_file_bytes', result['peak_wal_file_bytes'], _LIMITS['wal_file_bytes'])
    )
    checks.extend(
        (
            f'checkpointers[{index}].longest_uncleared_intervals',
            checkpoint['longest_uncleared_intervals'],
            _LIMITS['uncleared_intervals'],
        )
        for index, checkpoint in enumerate(result['checkpointers'])
    )
    return checks


def _hard_failures(result: dict[str, Any]) -> list[str]:
    """List breached limits and zero-tolerance process conditions."""
    failures = [name for name, value, limit in _limit_checks(result) if value > limit]
    if result['achieved_ratio'] < _LIMITS['achieved_ratio']:
        failures.append('achieved_ratio')
    if result['backlog_at_end']:
        failures.append('backlog_at_end')
    if result['busy_timeouts'] or any(result['expired_attempts'].values()):
        failures.append('timeouts')
    if any(checkpoint['bound_crossings'] for checkpoint in result['checkpointers']):
        failures.append('checkpoint_bound_crossings')
    if any(checkpoint['halt'] for checkpoint in result['checkpointers']):
        failures.append('checkpoint_halt')
    if any(process['exit_status'] for process in result['processes']):
        failures.append('process_exit_status')
    return failures


def _linux_filesystem_type(path: Path) -> str | None:
    """Find the longest matching Linux mount point for the work directory."""
    if sys.platform != 'linux':
        return None
    resolved = str(path.resolve())
    best = ('', None)
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        before, separator, after = line.partition(' - ')
        if not separator:
            continue
        fields = before.split()
        mount = fields[4].replace('\\040', ' ')
        if (resolved == mount or resolved.startswith(f'{mount.rstrip("/")}/')) and len(
            mount
        ) > len(best[0]):
            best = (mount, after.split()[0])
    return best[1]


def _merge_worst_checkpointers(worst: dict[str, Any], result: dict[str, Any]) -> None:
    """Retain each checkpointer's longest unclear period or halt."""
    for index, checkpointer in enumerate(result['checkpointers']):
        if index >= len(worst['checkpointers']):
            worst['checkpointers'].append(checkpointer)
            continue
        earlier = worst['checkpointers'][index]
        earlier['longest_uncleared_intervals'] = max(
            earlier['longest_uncleared_intervals'], checkpointer['longest_uncleared_intervals']
        )
        earlier['halt'] = earlier['halt'] or checkpointer['halt']
        earlier['bound_crossings'] = max(
            earlier['bound_crossings'], checkpointer['bound_crossings']
        )


def _merge_worst_latencies(worst: dict[str, Any], result: dict[str, Any]) -> None:
    """Raise each latency percentile to the worst repeat observation."""
    for name in _AGGREGATED_LATENCY_METRICS:
        if name not in worst or name not in result:
            continue
        if worst[name] is not None and result[name] is not None:
            for metric, value in result[name].items():
                worst[name][metric] = max(worst[name].get(metric, 0), value)
    for kind, metrics in result.get('commit_latency_ms_by_class', {}).items():
        earlier = worst.setdefault('commit_latency_ms_by_class', {}).setdefault(kind, {})
        for metric, value in metrics.items():
            earlier[metric] = max(earlier.get(metric, 0), value)


def _metric_list(value: object, path: Path, name: str) -> list[Any]:
    """Require a JSON array before inspecting retained records."""
    if not isinstance(value, list):
        raise _InvalidResultError(f'{path}: invalid result {name}: expected list')
    return cast('list[Any]', value)


def _metric_mapping(value: object, path: Path, field: str) -> dict[str, Any]:
    """Require a JSON object at an evaluator-consumed measurement boundary."""
    if not isinstance(value, dict):
        raise _InvalidResultError(f'{path}: invalid result {field}: expected object')
    return cast('dict[str, Any]', value)


def _metric_number(value: object, path: Path, field: str) -> None:
    """Reject nonnumeric, negative, or nonfinite external measurements."""
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or value < 0
        or (isinstance(value, float) and not math.isfinite(value))
    ):
        raise ValueError(f'{path}: invalid result {field}: expected finite nonnegative number')


def _offered_arrivals(spec: _ProcessSpec) -> list[dict[str, Any]]:
    """Derive identities after measurement from the original immutable schedules."""
    scenario = _SCENARIOS[spec.scenario]
    duration = spec.end_at - spec.start_at
    schedules = {group: scenario.segments(group, duration) for group in spec.lanes}
    if spec.poller:
        schedules['poller'] = [(0, duration, scenario.poller_rate)]
    if spec.lifecycle:
        schedules['lifecycle'] = [(0, duration, 1)]
    return [
        {
            'arrival_id': f'{spec.role}:{writer}:{ordinal}',
            'writer': writer,
            'scheduled_at': scheduled,
        }
        for writer, segments in schedules.items()
        for ordinal, scheduled in enumerate(_arrival_times(segments, spec.start_at, spec.end_at))
    ]


def _overlap_chains(intervals: list[tuple[float, float]]) -> tuple[int, float]:
    """Find the largest transitive overlap component and its total time span."""
    best = (0, 0.0)
    first = last = 0.0
    count = 0
    for start, end in sorted(intervals):
        if count and start < last:
            count += 1
            last = max(last, end)
        else:
            best = max(best, (count, last - first))
            first, last, count = start, end, 1
    return max(best, (count, last - first))


def _percentiles(values: list[float]) -> dict[str, float]:
    """Compute nearest-rank latency quantiles, using zero for an empty sample."""
    ordered = sorted(values)
    if not ordered:
        return dict.fromkeys(('p50', 'p99', 'p999', 'max'), 0.0)
    return {
        name: ordered[math.ceil(quantile * len(ordered)) - 1]
        for name, quantile in (('p50', 0.5), ('p99', 0.99), ('p999', 0.999), ('max', 1.0))
    }


def _metric(commits: list[dict[str, Any]], finish: str, start: str) -> dict[str, float]:
    """Summarize one pair of recorded phase boundaries in milliseconds."""
    return _percentiles([(record[finish] - record[start]) * _MILLISECONDS for record in commits])


def _capacity_summary(cell: _Cell, records: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Summarize capacity replay shape and latency only for its applicable cells."""
    if cell.population != 'cap-stress' or cell.scenario != 'lifecycle':
        return None
    replays = [replay for record in records for replay in record['capacity_replays']]
    return {
        'count': len(replays),
        'latency_ms': _metric(replays, 'finished_at', 'scheduled_at'),
        'min_late_ids_before': min((replay['late_ids'] for replay in replays), default=0),
        'min_pending_before': min((replay['pending'] for replay in replays), default=0),
        'restore_commits': sum(record['restore_commits'] for record in records),
    }


def _persist_cell_result(result: dict[str, Any], output: Path) -> None:
    """Copy a portable evidence bundle and save relative manifest paths after measurement."""
    persisted = json.loads(json.dumps(result))
    directory = output.parent / f'{output.stem}-evidence'
    directory.mkdir(parents=True, exist_ok=True)
    for entry in persisted.get('record_manifest', []):
        for kind in ('spec', 'raw'):
            source = Path(entry[f'{kind}_path'])
            target = directory / f'{entry["role"]}-{kind}.json'
            if source.exists():
                shutil.copyfile(source, target)
            entry[f'{kind}_path'] = str(target.relative_to(output.parent))
    output.write_text(json.dumps(persisted, indent=2) + '\n')


def _poller_close(connection: sqlite3.Connection, closed: tuple[str, list[int]]) -> None:
    """Close one expired range and record its deterministic late ids."""
    obligation, event_ids = closed
    connection.execute(
        _SQL_POLLER_CLOSE_UPDATE_DISCOVERY_RANGES,
        {'obligation': obligation},
    )
    connection.executemany(
        _SQL_POLLER_CLOSE_INSERT_SOURCE_LATE_IDS,
        [(obligation, event_id) for event_id in event_ids],
    )


def _poller_merge(connection: sqlite3.Connection, merged: tuple[str, int], now: float) -> None:
    """Extend one open range and reconsider its group obligations."""
    obligation, high = merged
    connection.execute(
        _SQL_POLLER_MERGE_UPDATE_DISCOVERY_RANGES,
        {'high': high, 'obligation': obligation},
    )
    connection.execute(
        _SQL_POLLER_MERGE_UPDATE_RANGE_GROUPS,
        {'obligation': obligation, 'high': high, 'now': now},
    )


def _poller_open(
    connection: sqlite3.Connection, opened: tuple[str, int, int, float], now: float
) -> None:
    """Insert one range and bind it to every non-orphaned group."""
    obligation, low, high, deadline = opened
    parameters = {
        'obligation': obligation,
        'low': low,
        'high': high,
        'now': now,
        'deadline': deadline,
    }
    connection.execute(_SQL_INSERT_RANGE, parameters)
    connection.execute(_SQL_BIND_RANGE, parameters)


def _poller_observe(
    connection: sqlite3.Connection, poller: _PollerState, plan: _PollPlan, now: float
) -> bool:
    """Apply a guarded source observation and report whether its commit needs a hint."""
    row = connection.execute(_SQL_SELECT_INDEX_REVISION).fetchone()
    if row is None or row[0] != poller.index_revision:
        raise _IndexChangedError
    connection.execute(
        _SQL_POLLER_OBSERVE_UPDATE_SOURCE_META,
        {'frontier': plan.frontier, 'now': now},
    )
    for opened in plan.opened:
        _poller_open(connection, opened, now)
    if plan.merged is not None:
        _poller_merge(connection, plan.merged, now)
    for closed in plan.closed:
        _poller_close(connection, closed)
    retired_ids = {str(row[0]) for row in connection.execute(_SQL_RETIRE).fetchall()}
    range_changed = bool(plan.opened or plan.merged is not None or plan.closed or retired_ids)
    if range_changed:
        connection.execute(BUMP_RANGE_REVISION)
    closed_ids = {obligation for obligation, _ in plan.closed}
    poller.open_ranges = [
        entry
        for entry in (*poller.open_ranges, *plan.opened)
        if entry[0] not in closed_ids and entry[0] not in retired_ids
    ]
    if plan.merged is not None:
        poller.open_ranges = [
            (identifier, low, plan.merged[1] if identifier == plan.merged[0] else high, deadline)
            for identifier, low, high, deadline in poller.open_ranges
        ]
    poller.next_range += len(plan.opened)
    poller.frontier = plan.frontier
    poller.cycle += 1
    return range_changed


def _population_counts(connection: sqlite3.Connection) -> dict[str, int]:
    """Count every schema table and each group's pending obligations."""
    counts = {
        table: int(connection.execute(sql).fetchone()[0]) for table, sql in _COUNT_SQL.items()
    }
    counts['open_ranges'] = int(
        connection.execute(_SQL_POPULATION_COUNTS_SELECT_DISCOVERY_RANGES).fetchone()[0]
    )
    counts.update(
        {
            f'pending:{group}': 0
            for (group,) in connection.execute(_SQL_POPULATION_COUNTS_SELECT_GROUPS)
        }
    )
    counts.update(
        {
            f'pending:{group}': int(count)
            for group, count in connection.execute(_SQL_SELECT_PENDING_RANGE_GROUP_COUNTS)
        }
    )
    return counts


def _population_summary(reads: Sequence[dict[str, Any]]) -> dict[str, dict[str, int]]:
    """Reduce recorded status counts to each tracked quantity's observed extrema."""
    values: dict[str, list[int]] = defaultdict(list)
    for record in reads:
        if record['kind'] == 'status' and record.get('outcome') != 'failed':
            for name, value in record['counts'].items():
                values[name].append(value)
    return {name: {'min': min(samples), 'max': max(samples)} for name, samples in values.items()}


def _process_arrival_evidence(
    spec: dict[str, Any], raw: dict[str, Any], duration_secs: float
) -> tuple[dict[str, float], dict[str, float]]:
    """Validate one retained process's offered and successful arrival identities."""
    offered: dict[str, float] = {}
    completed: dict[str, float] = {}
    start, end = spec['start_at'], spec['end_at']
    for name, value in [
        ('start_at', start),
        ('end_at', end),
        ('finished_at', raw['finished_at']),
    ]:
        _metric_number(value, Path('<evidence>'), name)
    if end - start != duration_secs or raw['finished_at'] < end:
        raise ValueError('incomplete measurement interval')
    arrivals = _metric_list(raw['offered_arrivals'], Path('<evidence>'), 'offered_arrivals')
    if type(raw['offered']) is not int or raw['offered'] != len(arrivals):
        raise ValueError('offered count does not match retained arrivals')
    local_ids: set[str] = set()
    for item in arrivals:
        arrival = _metric_mapping(item, Path('<evidence>'), 'arrival')
        identity, scheduled = arrival['arrival_id'], arrival['scheduled_at']
        _metric_number(scheduled, Path('<evidence>'), 'scheduled_at')
        if (
            not isinstance(identity, str)
            or not identity.startswith(f'{raw["role"]}:')
            or identity in offered
            or not start <= scheduled < end
        ):
            raise ValueError('invalid or duplicate offered arrival')
        offered[identity] = scheduled
        local_ids.add(identity)
    for item in _metric_list(raw['commits'], Path('<evidence>'), 'commits'):
        commit = _metric_mapping(item, Path('<evidence>'), 'commit')
        if commit.get('maintenance') is True:
            continue
        identity, finished = commit['arrival_id'], commit['finished_at']
        _metric_number(finished, Path('<evidence>'), 'commit.finished_at')
        if identity not in local_ids or identity in completed or finished < offered[identity]:
            raise ValueError('unaccounted or duplicate completed arrival')
        completed[identity] = finished
    return offered, completed


def _process_record_fields(runtime: _Runtime) -> dict[str, Any]:
    """Build ordered process fields with incremental retained-record iterators."""
    arrivals = _offered_arrivals(runtime.spec)
    identities = {(item['writer'], item['scheduled_at']): item['arrival_id'] for item in arrivals}

    def commits() -> Iterator[dict[str, Any]]:
        for commit in runtime.commits.records():
            if not commit.get('maintenance'):
                commit['arrival_id'] = identities.get(
                    (commit['writer'], commit['scheduled_at']), commit.get('arrival_id')
                )
            yield commit

    evidence_errors = (
        [f'diagnostic overflow: {runtime.diagnostic_overflow}']
        if runtime.diagnostic_overflow
        else []
    )
    if runtime.phases.overflow:
        evidence_errors.append(f'phase overflow: {runtime.phases.overflow}')
    return {
        'phases': runtime.phases.records(),
        'phase_cap': runtime.phases.capacity,
        'phase_overflow': runtime.phases.overflow,
        'role': runtime.spec.role,
        'lanes': runtime.spec.lanes,
        'exit_status': int(bool(runtime.errors)),
        'errors': runtime.errors,
        'commits': commits(),
        'offered_arrivals': arrivals,
        'diagnostics': runtime.diagnostics.records(),
        'diagnostic_cap': runtime.spec.diagnostic_cap,
        'diagnostic_overflow': runtime.diagnostic_overflow,
        'extra_timing': runtime.spec.extra_timing,
        'evidence_complete': not evidence_errors,
        'evidence_errors': evidence_errors,
        'reads': runtime.reads.records(),
        'checkpoints': runtime.checkpoints,
        'checkpoint_halt': runtime.checkpoint_halt,
        'checkpoint_started_at': runtime.checkpoint_started_at,
        'checkpoint_finished_at': runtime.checkpoint_finished_at,
        'checkpoint_crossings': runtime.checkpoint_crossings,
        'checkpoint_owner': runtime.spec.poller,
        'expired_attempts': dict(Counter(runtime.expired_attempts)),
        'busy_timeouts': runtime.busy_timeouts,
        'offered': len(arrivals),
        'index_reloads': runtime.index_reloads,
        'capacity_replays': runtime.capacity_replays,
        'restore_commits': runtime.restore_commits,
        'peak_rss_bytes': max((rss for rss, _ in runtime.samples), default=0),
        'peak_open_fds': max((fds for _, fds in runtime.samples), default=0),
        'finished_at': runtime.finished_at,
        'health_projection_reads': sum(
            record['kind'] == 'health-projection' for record in runtime.reads
        ),
    }


def _process_records(runtime: _Runtime) -> dict[str, Any]:
    """Materialize process records for in-memory callers after measurement."""
    return {
        key: list(cast('Iterator[dict[str, Any]]', value))
        if isinstance(value, Iterator)
        else value
        for key, value in _process_record_fields(runtime).items()
    }


def _publish_range_hints(fds: tuple[int, ...]) -> None:
    """Notify every lane after commit, dropping hints when a pipe is already full."""
    for fd in fds:
        with suppress(BlockingIOError):
            os.write(fd, b'\0')


def _qualification_failures(
    worst: dict[str, Any], failures: list[str], *, capacity_only: bool
) -> tuple[list[str], list[str]]:
    """Partition capacity and binding failures, separating busy timeouts from expiries."""
    capacity = [
        name
        for name in failures
        if capacity_only
        and (
            name.startswith(
                (
                    'commit_latency_ms.',
                    'all_commit_latency_ms.',
                    'full_lifecycle_latency_ms.',
                    'checkpointers[',
                )
            )
            or name in {'achieved_ratio', 'backlog_at_end', 'peak_wal_file_bytes'}
        )
    ]
    safety = [name for name in failures if name not in capacity and name != 'timeouts']
    if capacity_only and worst['peak_wal_file_bytes'] > WAL_IN_USE_LIMIT_BYTES:
        safety.append('wal_runtime_size')
    if worst['busy_timeouts']:
        safety.append('busy_timeouts')
    if any(worst['expired_attempts'].values()):
        (capacity if capacity_only else safety).append('expired_attempts')
    return capacity, safety


def _qualification_summary(evaluations: list[dict[str, Any]]) -> dict[str, Any]:
    """Label selected-cell outcomes without claiming complete deployment qualification."""
    qualification = evaluations[0]['qualification']
    return {
        'qualification': qualification,
        'qualification_label': (
            'NORMAL on m7i-flex.large; FULL capacity not qualified'
            if qualification == 'normal-m7i-flex.large'
            else 'all modes'
        ),
        'qualification_verdict': 'fail' if _evaluation_status(evaluations) else 'pass',
        'matrix_complete': {item['cell'] for item in evaluations}
        == set(range(1, len(_CELLS) + 1)),
        'repeat': [item['cell'] for item in evaluations if item['repeat_required']],
    }


def _print_verdicts(evaluations: list[dict[str, Any]]) -> None:
    """Print raw and selected-scope verdicts with explicit matrix coverage."""
    summary = _qualification_summary(evaluations)
    print(summary['qualification_label'])
    print('Cell  Raw           Qualification  Runs  Repeat required')
    for evaluation in evaluations:
        print(
            f'{evaluation["cell"]:>4}  {evaluation["verdict"]:<12}  '
            f'{evaluation["qualification_verdict"]:<13}  {evaluation["runs"]}  '
            f'{evaluation["repeat_required"]}'
        )
        if evaluation['failures']:
            print(f'      failures: {", ".join(evaluation["failures"])}')
        if evaluation.get('evidence_errors'):
            print(f'      evidence: {", ".join(evaluation["evidence_errors"])}')
    if not summary['matrix_complete']:
        print('Selected cells only: incomplete matrix; no complete qualification.')
    elif summary['qualification'] == 'normal-m7i-flex.large':
        print('Storage matrix only; live load and host probe acceptance are separate checks.')


def _reader_summary(reads: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize read transaction durations and transitive concurrency."""
    count, span = _overlap_chains([(record['start'], record['end']) for record in reads])
    return {
        'durations_ms': {
            kind: _percentiles(
                [
                    (record['end'] - record['start']) * _MILLISECONDS
                    for record in reads
                    if record['kind'] == kind
                ]
            )
            for kind in ('health-projection', 'snapshot', 'status')
        },
        'longest_overlap_chain': {'readers': count, 'span_secs': span},
    }


def _reconcile_arrival_evidence(
    result: dict[str, Any], records: list[tuple[dict[str, Any], dict[str, Any]]]
) -> None:
    """Recompute load and drain counts from unique offered and committed arrival identities."""
    offered: dict[str, float] = {}
    completed: dict[str, float] = {}
    ends: set[float] = set()
    for spec, raw in records:
        local_offered, local_completed = _process_arrival_evidence(
            spec, raw, result['duration_secs']
        )
        offered.update(local_offered)
        completed.update(local_completed)
        ends.add(spec['end_at'])
    if len(ends) != 1 or not offered:
        raise ValueError('missing or inconsistent measurement boundaries')
    end = ends.pop()
    for name, boundary in [('load_end', end), ('drain_end', end + _DRAIN_SECS)]:
        timely = {identity for identity, finished in completed.items() if finished <= boundary}
        outstanding = set(offered) - timely
        counts = result['arrival_accounting'][name]
        for key, value in [
            ('offered', len(offered)),
            ('completed', len(timely)),
            ('outstanding', len(outstanding)),
        ]:
            if type(counts[key]) is not int or counts[key] != value:
                raise ValueError(f'{name}: inconsistent {key}')
        if (
            not isinstance(counts['outstanding_ids'], list)
            or len(counts['outstanding_ids']) != len(outstanding)
            or set(counts['outstanding_ids']) != outstanding
        ):
            raise ValueError(f'{name}: unaccounted arrivals')
    drain = result['arrival_accounting']['drain_end']
    if (
        result['backlog_at_end'] != drain['outstanding']
        or result['offered_commits'] != drain['offered']
        or result['achieved_commits'] != drain['completed']
        or result['achieved_ratio'] != drain['completed'] / drain['offered']
    ):
        raise ValueError('aggregate arrival metrics do not reconcile')


def _record_checkpoint(runtime: _Runtime, attempt: CheckpointAttempt, frame_bytes: int) -> None:
    """Retain a classified checkpoint, including a bound-crossing attempt."""
    runtime.checkpoints.append(
        {
            **asdict(attempt),
            'frame_bytes': frame_bytes,
            'wal_file_bytes': attempt.wal_bytes,
        }
    )


def _record_diagnostic(
    runtime: _Runtime, record: dict[str, Any], *, required: bool = False
) -> None:
    """Append within the per-process cap, counting every omitted diagnostic."""
    if not runtime.spec.extra_timing and not required:
        return
    record.update(
        role=runtime.spec.role,
        pid=os.getpid(),
        thread_id=threading.get_ident(),
        thread_name=threading.current_thread().name,
    )
    with runtime.diagnostic_lock:
        if not runtime.diagnostics.append_if_room(record, runtime.spec.diagnostic_cap):
            runtime.diagnostic_overflow += 1


@contextmanager
def _diagnostic_span(runtime: _Runtime, kind: str, **details: object) -> Generator[dict[str, Any]]:
    """Retain named boundaries on success and exception without changing the operation."""
    record: dict[str, Any] = {'kind': kind, **details}
    if not runtime.spec.extra_timing:
        try:
            yield record
        finally:
            if kind == 'writer-attempt' and record.get('outcome') != 'committed':
                _record_diagnostic(runtime, record, required=True)
        return
    record['start'] = _clock_sample()
    record['outcome'] = 'failed'
    try:
        yield record
        if record['outcome'] == 'failed':
            record['outcome'] = 'completed'
    finally:
        record['end'] = _clock_sample()
        _record_diagnostic(runtime, record)


def _checkpoint_factory(runtime: _Runtime, frame_bytes: int) -> Callable[..., Checkpointer]:
    """Record production host attempts and newly crossed bounds without changing policy."""

    class _Observed(Checkpointer):
        """Retain the real checkpointer's outcome and bound transitions."""

        def close(self) -> None:
            """Record the owner thread's final close, even after an attempt failure."""
            try:
                super().close()
            finally:
                runtime.checkpoint_finished_at = time.monotonic()

        def _checkpoint(self, mode: Literal['PASSIVE', 'TRUNCATE']) -> tuple[int, int, int]:
            """Observe the SQLite call and result fetch, excluding timeout setup/restore."""
            with runtime.phases.span(mode.lower(), self._phase_id):
                return super()._checkpoint(mode)

        def _sync(self) -> None:
            """Observe fallback WAL sync, including its lazy descriptor open if needed."""
            with runtime.phases.span('sync', self._phase_id):
                super()._sync()

        def run_once(self) -> CheckpointAttempt:
            """Run production maintenance and append this attempt's gate measurements.

            Returns:
                The production checkpoint result after its measurements are
                recorded.

            Raises:
                DatabaseError: A SQLite checkpoint operation fails.
                OSError: The WAL file cannot be inspected or synchronized.
                OverflowError: Retained diagnostic records exceed the unsigned
                    offset representation.
            """
            crossed = self.crossed
            self._phase_id = runtime.phases.identity()
            with _diagnostic_span(runtime, 'checkpoint') as record:
                if runtime.spec.extra_timing:
                    record['phase_id'] = self._phase_id
                attempt = super().run_once()
            runtime.checkpoint_crossings += len(self.crossed - crossed)
            _record_checkpoint(runtime, attempt, frame_bytes)
            return attempt

    def create(*args: object, **kwargs: object) -> Checkpointer:
        checkpointer = cast('Callable[..., Checkpointer]', _Observed)(*args, **kwargs)
        runtime.checkpoint_started_at = time.monotonic()
        return checkpointer

    return create


@contextmanager
def _diagnostic_session(runtime: _Runtime) -> Generator[None]:
    """Scope GC callbacks to this process run and retain process boundary CPU samples."""
    if not runtime.spec.extra_timing:
        yield
        return

    def callback(phase: str, info: dict[str, int]) -> None:
        _record_diagnostic(
            runtime,
            {'kind': 'gc', 'phase': phase, 'generation': info['generation'], **_clock_sample()},
        )

    gc.callbacks.append(callback)
    try:
        with _diagnostic_span(runtime, 'process'):
            yield
    finally:
        gc.callbacks.remove(callback)


@contextmanager
def _read_transaction(connection: sqlite3.Connection) -> Generator[None]:
    """Close one deferred read transaction even when a query fails."""
    connection.execute(_SQL_READ_TRANSACTION_BEGIN_TRANSACTION)
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def _capacity_counts(connection: sqlite3.Connection) -> dict[str, int]:
    """Read the restored group's late-id and pending shape in one snapshot."""
    with _read_transaction(connection):
        late = int(
            connection.execute(
                'SELECT COUNT(*) FROM late_ids WHERE group_name=?', (_CAPACITY_GROUP,)
            ).fetchone()[0]
        )
        pending = int(
            connection.execute(
                'SELECT COUNT(*) FROM range_groups WHERE group_name=? '
                "AND obligation_state='pending'",
                (_CAPACITY_GROUP,),
            ).fetchone()[0]
        )
    return {'late_ids': late, 'pending': pending}


def _health_projection_read(
    connection: sqlite3.Connection,
    group_name: str,
    *,
    late_ids: tuple[int, list[int]] | None = None,
) -> tuple[int, list[int], int, int]:
    """Read current pending/audit counts with refreshed or cached late-ID display."""
    parameters = {'group': group_name}
    with _read_transaction(connection):
        if late_ids is None:
            missing_count = connection.execute(
                UNRESOLVED_LATE_ID_COUNT_SQL, (group_name,)
            ).fetchone()[0]
            missing_prefix = [
                row[0]
                for row in connection.execute(UNRESOLVED_LATE_ID_PREFIX_SQL, (group_name, 100))
            ]
        else:
            missing_count, missing_prefix = late_ids
        pending = connection.execute(
            _SQL_HEALTH_PROJECTION_READ_SELECT_RANGE_GROUPS,
            parameters,
        ).fetchone()[0]
        audit = connection.execute(
            _SQL_HEALTH_PROJECTION_READ_SELECT_AUDIT, parameters
        ).fetchone()[0]
    return missing_count, missing_prefix, pending, audit


def _lane_refresh(connection: sqlite3.Connection, lane: _LaneState) -> bool:
    """Reload a lane's pending ranges after a range revision or generation change."""
    with _read_transaction(connection):
        revision = int(connection.execute(_SQL_LANE_REFRESH_SELECT_SOURCE_META).fetchone()[0])
        generation = int(
            connection.execute(
                _SQL_SELECT_GROUP_GENERATION,
                {'group': lane.group_name},
            ).fetchone()[0]
        )
        if revision == lane.revision_seen and generation == lane.generation:
            return False
        sources = _lane_source_ranges(connection, lane, generation)
    previous = {work.obligation_id: work for work in lane.scan_work}
    lane.scan_work = [
        previous[obligation]
        if obligation in previous and source is lane.source_ranges.get(obligation)
        else _ScanWork(obligation, list(source.late_ids))
        for obligation, source in sources.items()
        if source.version[2] == 'closed' and obligation not in lane.scan_suppressed
    ]
    lane.source_ranges = sources
    lane.generation = generation
    lane.revision_seen = revision
    return True


def _lane_prepare(
    connection: sqlite3.Connection,
    lane: _LaneState,
    *,
    now: float,
    hinted: bool = False,
    force: bool = False,
) -> None:
    """Service due coordination and rearm from completion, including replay recovery."""
    if now < lane.next_coordination_at and not hinted and not force:
        return
    generation = lane.generation
    try:
        _lane_refresh(connection, lane)
        if lane.generation != generation:
            _lane_resume(connection, lane)
            lane.arrivals = 0
            lane.epoch_turn = 0
            lane.next_snapshot_at = 0.0
            lane.health_late_ids = None
    finally:
        lane.next_coordination_at = time.monotonic() + _FETCH_INTERVAL_SECS


def _poller_reload(connection: sqlite3.Connection, poller: _PollerState) -> None:
    """Reload the poller's full source index in one read transaction."""
    with _read_transaction(connection):
        poller.index_revision = int(connection.execute(_SQL_SELECT_INDEX_REVISION).fetchone()[0])
        rows = connection.execute(_SQL_POLLER_RELOAD_SELECT_DISCOVERY_RANGES).fetchall()
        connection.execute(_SQL_POLLER_RELOAD_SELECT_RANGE_GROUPS).fetchall()
        connection.execute(_SQL_POLLER_RELOAD_SELECT_REPLAY_MARKERS).fetchall()
        connection.execute(_SQL_POLLER_RELOAD_SELECT_GROUPS).fetchall()
        poller.frontier = int(connection.execute(_SQL_SELECT_FRONTIER).fetchone()[0])
    poller.open_ranges = [
        (str(identifier), int(low), int(high), float(deadline))
        for identifier, low, high, _, deadline, state in rows
        if state == 'open'
    ]
    poller.next_range = max(
        (
            int(identifier[6:]) + 1
            for identifier, *_ in poller.open_ranges
            if identifier.startswith('bench-')
        ),
        default=poller.next_range,
    )


@contextmanager
def _range_hint_pipe(database_path: str, group: str) -> Generator[int]:
    """Open one ephemeral body-free notification pipe shared by the cell's processes."""
    path = Path(f'{database_path}.{group}.range-hint')
    with suppress(FileExistsError):
        os.mkfifo(path, mode=0o600)
    fd = os.open(path, os.O_RDWR | os.O_NONBLOCK)
    try:
        yield fd
    finally:
        os.close(fd)


def _record_read(runtime: _Runtime, kind: str, operation: Callable[[], object]) -> object:
    """Time complete reads, retaining both successful and exceptional exits."""
    started = time.monotonic()
    result: object = None
    outcome = 'failed'
    try:
        with _diagnostic_span(runtime, 'read', reader=kind):
            result = operation()
        outcome = 'completed'
        return result
    finally:
        runtime.reads.append(
            {
                'kind': kind,
                'start': started,
                'end': time.monotonic(),
                'counts': result,
                'outcome': outcome,
            }
        )


def _record_rows(raw: dict[str, Any], name: str) -> list[dict[str, Any]]:
    """Require an array of objects without normalizing malformed evidence."""
    return [
        _metric_mapping(item, Path('<records>'), name)
        for item in _metric_list(raw[name], Path('<records>'), name)
    ]


def _record_strings(value: object, name: str) -> None:
    """Require an actual string array at a retained-record boundary."""
    values = _metric_list(value, Path('<records>'), name)
    if any(not isinstance(item, str) for item in values):
        raise ValueError(f'invalid {name}: expected strings')


def _repeat_identity(result: dict[str, Any]) -> dict[str, Any]:
    """Require the complete recorded measurement identity before pooling repeats."""
    fields = (
        'environment',
        'cell',
        'model',
        'population',
        'duration_secs',
        'duration_override',
        'min_samples',
        'checkpoint_interval_secs',
    )
    if result['cell']['scenario'] == 'catchup':
        fields += ('catchup_burst_secs',)
    missing = [field for field in fields if field not in result]
    if missing:
        raise ValueError(f'missing repeat identity fields: {", ".join(missing)}')
    for name in ('environment', 'model', 'population'):
        if not isinstance(result[name], dict) or not result[name]:
            raise ValueError(f'invalid repeat identity {name}')
    identity = {field: result[field] for field in fields}
    scenario = _SCENARIOS[result['cell']['scenario']]
    nominal = _default_duration(scenario, _MIN_SAMPLES)
    if not result['duration_override'] and math.isclose(
        result['duration_secs'], nominal, rel_tol=0.0, abs_tol=1e-6
    ):
        identity['duration_secs'] = nominal
    return identity


def _result_eligible(result: dict[str, Any]) -> bool:
    """Require default load, complete startup, and sustained population shape."""
    cell = result['cell']
    population = result['population']
    scenario = _SCENARIOS[cell['scenario']]
    duration = _default_duration(scenario, _MIN_SAMPLES)
    if (
        (
            result['duration_secs'] < duration
            and not math.isclose(result['duration_secs'], duration, rel_tol=0.0, abs_tol=1e-6)
        )
        or result.get('duration_override', False)
        or result['min_samples'] != _MIN_SAMPLES
        or result['checkpoint_interval_secs'] != DEFAULT_CHECKPOINT_INTERVAL_SECS
        or not population.get('gate_eligible', False)
        or population.get('population') != cell['population']
        or any(process['exit_status'] for process in result['processes'])
        or any('late-start' in process['errors'] for process in result['processes'])
        or len(result['checkpointers']) != 1
        or any(
            checkpoint['halt'] is not None
            or checkpoint['host_started_at'] is None
            or checkpoint['host_started_at'] > result['start_at']
            or checkpoint['host_finished_at'] is None
            or checkpoint['host_finished_at'] < result['end_at']
            for checkpoint in result['checkpointers']
        )
    ):
        return False
    if cell['scenario'] == 'catchup' and result.get('catchup_burst_secs', 0) < _BURST_TOTAL_SECS:
        return False
    if cell['population'] == 'representative':
        return population.get('warmup_secs', 0) >= _MIN_DURATION_SECS
    shape = result['population_during_run']
    replay = result['capacity_replays']
    return _capacity_shape_eligible(shape, replay, result['duration_secs'])


def _retained_process_records(
    result: dict[str, Any],
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Verify expected roles and hashes before consuming retained process evidence."""
    expected = dict(_TOPOLOGIES[result['cell']['topology']])
    manifest = _metric_list(result['record_manifest'], Path('<evidence>'), 'record_manifest')
    if len(manifest) != len(expected):
        raise ValueError('incomplete record_manifest')
    records: list[tuple[dict[str, Any], dict[str, Any]]] = []
    seen: set[str] = set()
    for item in manifest:
        entry = _metric_mapping(item, Path('<evidence>'), 'manifest entry')
        role = entry['role']
        if role not in expected or role in seen:
            raise ValueError('unexpected or duplicate manifest role')
        seen.add(role)
        pair: list[dict[str, Any]] = []
        for kind in ('spec', 'raw'):
            content = Path(entry[f'{kind}_path']).read_bytes()
            if hashlib.sha256(content).hexdigest() != entry[f'{kind}_sha256']:
                raise ValueError(f'{role}: {kind} hash mismatch')
            data = _metric_mapping(json.loads(content), Path(entry[f'{kind}_path']), kind)
            if data['role'] != role or tuple(data['lanes']) != expected[role]:
                raise ValueError(f'{role}: {kind} identity mismatch')
            pair.append(data)
        records.append((pair[0], pair[1]))
    return records


def _retention_summary(run: _CellRun, records: list[dict[str, Any]]) -> dict[str, Any]:
    """Check every expected process output against the exact collected records."""
    errors: list[str] = []
    manifest: list[dict[str, Any]] = []
    if run.directory is None:
        return {
            'evidence_complete': False,
            'evidence_errors': ['no retained directory'],
            'record_manifest': manifest,
        }
    specs = _cell_specs(run, run.directory)
    by_role = {record['role']: record for record in records}
    if len(records) != len(specs) or len(by_role) != len(records):
        errors.append('incomplete or duplicate collected process roles')
    for spec in specs:
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            'child',
            '--spec',
            str(run.directory / f'{spec.role}-spec.json'),
        ]
        entry: dict[str, Any] = {
            'role': spec.role,
            'command': command if spec.role != 'site' else [sys.executable, *sys.argv],
            'entry_point': '_run_process',
        }
        for kind, path in [
            ('spec', run.directory / f'{spec.role}-spec.json'),
            ('raw', Path(spec.output_path)),
        ]:
            entry[f'{kind}_path'] = str(path.resolve())
            try:
                content = path.read_bytes()
                entry[f'{kind}_sha256'] = hashlib.sha256(content).hexdigest()
            except OSError as exc:
                errors.append(f'{spec.role}: {kind}: {exc}')
        raw = by_role.get(spec.role, {})
        errors.extend(f'{spec.role}: {error}' for error in raw.get('evidence_errors', []))
        if raw.get('errors') or raw.get('exit_status'):
            errors.append(f'{spec.role}: process failed')
        manifest.append(entry)
    return {
        'evidence_complete': not errors,
        'evidence_errors': errors,
        'record_manifest': manifest,
    }


def _retryable_failure(runtime: _Runtime, timing: WriteTiming, exc: DatabaseError) -> bool:
    """Count only acquisition failures as recoverable without advancing model state."""
    if not math.isnan(timing.begun_at):
        return False
    if isinstance(exc, WriteLockTimeoutError):
        runtime.expired_attempts.append(exc.level)
        return True
    if 'database is locked' in str(exc).lower():
        runtime.busy_timeouts += 1
        return True
    return isinstance(exc, WriteLockStoppedError) and runtime.stop.is_set()


def _revision(connection: sqlite3.Connection, group_name: str) -> int:
    """Fetch the current group-state revision for an optimistic update."""
    row = connection.execute(_SQL_REVISION_SELECT_GROUP_STATE, {'group': group_name}).fetchone()
    if row is None:
        raise ValueError(f'unknown group: {group_name}')
    return int(row[0])


def _lane_cursor(connection: sqlite3.Connection, lane: _LaneState) -> None:
    """Advance a delivered event after the modeled strict-fence read."""
    if lane.strict:
        connection.execute(
            _SQL_LANE_CURSOR_SELECT_RANGE_GROUPS,
            {'group': lane.group_name, 'generation': lane.generation},
        ).fetchone()
    revision = _revision(connection, lane.group_name)
    event = lane.in_flight
    if event is None:
        raise ValueError('cursor advance requires an in-flight event')
    connection.execute(
        _SQL_LANE_CURSOR_UPDATE_GROUP_STATE,
        {'group': lane.group_name, 'event': event, 'revision': revision},
    )
    connection.execute(
        _SQL_LANE_CURSOR_DELETE_CALLBACK_MARKS,
        {'group': lane.group_name, 'event': event},
    )
    lane.in_flight = None
    lane.next_event_id += 1


def _lane_late_mark(
    connection: sqlite3.Connection, lane: _LaneState, work: _ScanWork, event_id: int
) -> None:
    """Record terminal delivery of one late id for a pending range."""
    connection.execute(
        _SQL_LANE_LATE_MARK_INSERT_LATE_IDS,
        {'group': lane.group_name, 'obligation': work.obligation_id, 'event': event_id},
    )
    revision = _revision(connection, lane.group_name)
    connection.execute(
        _SQL_LANE_LATE_MARK_UPDATE_GROUP_STATE,
        {'group': lane.group_name, 'revision': revision},
    )


def _lane_mark(connection: sqlite3.Connection, lane: _LaneState) -> None:
    """Mark one callback delivery in the current write transaction."""
    event = lane.next_event_id
    revision = _revision(connection, lane.group_name)
    connection.execute(
        _SQL_LANE_MARK_UPDATE_GROUP_STATE,
        {'group': lane.group_name, 'event': event, 'revision': revision},
    )
    connection.execute(
        _SQL_LANE_MARK_INSERT_CALLBACK_MARKS,
        {'group': lane.group_name, 'event': event},
    )
    lane.in_flight = event


def _run_thread_job(runtime: _Runtime, job: _ThreadJob) -> None:
    """Open and close the connection on its owning workload thread."""
    handle = open_database(
        Path(runtime.spec.database_path),
        source_identity('bench', 'source'),
        SOURCE_SCHEMA,
        synchronous=job.synchronous,
    )
    try:
        if job.name == 'restore':
            runtime.capacity.counts = _capacity_counts(handle.connection)
            runtime.capacity.ready.set()
        job.ready.set()
        runtime.go.wait()
        if not runtime.stop.is_set():
            job.run(runtime, handle.connection)
    finally:
        handle.close()


def _seed_audit(connection: sqlite3.Connection) -> None:
    """Create audit history for all active and orphaned groups."""
    groups = (*_GROUP_NAMES, _ORPHAN_GROUP)
    for group in groups:
        connection.executemany(
            _SQL_SEED_AUDIT_INSERT_AUDIT,
            [(group, os.getuid(), os.getpid()) for _ in range(10_000)],
        )


def _seed_base(connection: sqlite3.Connection, *, strict_groups: tuple[str, ...]) -> None:
    """Insert the minimal thirteen-group source population."""
    connection.execute(_SQL_SEED_BASE_INSERT_SOURCE_META)
    connection.executemany(
        _SQL_SEED_BASE_INSERT_GROUPS,
        [(group, 'strict' if group in strict_groups else 'eager') for group in _GROUP_NAMES],
    )
    connection.executemany(
        _SQL_SEED_BASE_INSERT_GROUP_STATE,
        [(group,) for group in _GROUP_NAMES],
    )


def _seed_closed_ranges(connection: sqlite3.Connection) -> None:
    """Fill pinned groups with closed obligations and source late ids."""
    closed_count = DEFAULT_MAX_GROUP_OBLIGATIONS - DEFAULT_MAX_OPEN_RANGES
    for index in range(closed_count):
        obligation = f'seed-closed-{index:03d}'
        low = index * 60 + 1
        connection.execute(
            _SQL_INSERT_RANGE,
            {
                'obligation': obligation,
                'low': low,
                'high': low + 1,
                'now': -_RANGE_LIFETIME_SECS,
                'deadline': 0.0,
            },
        )
        connection.execute(
            _SQL_SEED_CLOSED_RANGES_UPDATE_DISCOVERY_RANGES,
            (obligation,),
        )
        connection.executemany(
            _SQL_SEED_CLOSED_RANGES_INSERT_RANGE_GROUPS,
            [
                (obligation, group, 'pending' if group in _PINNED_GROUPS else 'scan-complete')
                for group in _GROUP_NAMES
            ],
        )
        first = index * DEFAULT_MAX_LATE_IDS_PER_GROUP // closed_count
        last = (index + 1) * DEFAULT_MAX_LATE_IDS_PER_GROUP // closed_count
        late = [(obligation, low + 2 + offset) for offset in range(last - first)]
        connection.executemany(
            _SQL_SEED_CLOSED_RANGES_INSERT_SOURCE_LATE_IDS,
            late,
        )
        connection.executemany(
            _SQL_SEED_CLOSED_RANGES_INSERT_LATE_IDS,
            [(group, oid, event_id) for group in _PINNED_GROUPS for oid, event_id in late],
        )


def _seed_open_ranges(connection: sqlite3.Connection) -> None:
    """Fill the open-range cap with deadlines spread across one lifetime."""
    for index in range(DEFAULT_MAX_OPEN_RANGES):
        obligation = f'seed-open-{index:03d}'
        low = 1_000_000 - 2 * DEFAULT_MAX_OPEN_RANGES + index * 2
        deadline = _RANGE_LIFETIME_SECS * (index + 1) / DEFAULT_MAX_OPEN_RANGES
        _poller_open(connection, (obligation, low, low + 1, deadline), 0.0)


def _seed_cap_stress(path: Path) -> None:
    """Seed, checkpoint, and close the documented cap-stress population."""
    handle = open_database(
        path, source_identity('bench', 'source'), SOURCE_SCHEMA, synchronous='NORMAL'
    )
    connection = handle.connection
    try:
        connection.execute(_SQL_BEGIN_IMMEDIATE)
        try:
            _seed_base(connection, strict_groups=_STRICT_GROUPS)
            connection.execute(_SQL_SEED_CAP_STRESS_UPDATE_SOURCE_META)
            connection.execute(_SQL_SEED_CAP_STRESS_INSERT_GROUPS)
            connection.execute(_SQL_SEED_CAP_STRESS_INSERT_GROUP_STATE)
            connection.execute(_SQL_SEED_CAP_STRESS_INSERT_REPLAY_MARKERS)
            _seed_open_ranges(connection)
            _seed_closed_ranges(connection)
            _seed_audit(connection)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        checkpoint = connection.execute(_SQL_WAL_CHECKPOINT_TRUNCATE).fetchone()
        if checkpoint is None or checkpoint[0] != 0:
            raise RuntimeError('cap-stress WAL checkpoint was busy')
    finally:
        handle.close()


def _snapshot_read(connection: sqlite3.Connection, group_name: str) -> None:
    """Read one group's periodic full health snapshot."""
    parameters = {'group': group_name}
    with _read_transaction(connection):
        connection.execute(_SQL_SNAPSHOT_READ_SELECT_GROUPS, parameters).fetchall()
        connection.execute(_SQL_SNAPSHOT_READ_SELECT_GROUP_STATE, parameters).fetchall()
        connection.execute(_SQL_SNAPSHOT_READ_SELECT_CALLBACK_MARKS, parameters).fetchall()
        connection.execute(_SQL_SNAPSHOT_READ_SELECT_EPOCHS, parameters).fetchall()
        connection.execute(
            _SQL_SNAPSHOT_READ_SELECT_LATE_IDS,
            parameters,
        ).fetchone()


def _lane_health(runtime: _Runtime, connection: sqlite3.Connection, lane: _LaneState) -> None:
    """Keep cheap fields current and refresh late-ID display on the projection grid."""
    now = time.monotonic()
    due = now >= lane.next_snapshot_at
    if due:
        _record_read(runtime, 'snapshot', lambda: _snapshot_read(connection, lane.group_name))

    def project() -> tuple[int, list[int], int, int]:
        projection = _health_projection_read(
            connection, lane.group_name, late_ids=None if due else lane.health_late_ids
        )
        lane.health_late_ids = projection[:2]
        return projection

    _record_read(runtime, 'health-projection', project)
    if due:
        if lane.next_snapshot_at <= 0.0:
            lane.next_snapshot_at = now + _HEALTH_INTERVAL_SECS
        else:
            intervals = math.floor((now - lane.next_snapshot_at) / _HEALTH_INTERVAL_SECS) + 1
            lane.next_snapshot_at += intervals * _HEALTH_INTERVAL_SECS


def _status_read(connection: sqlite3.Connection) -> dict[str, int]:
    """Read operator status and return tracked population counts."""
    with _read_transaction(connection):
        meta = connection.execute(_SQL_STATUS_READ_SELECT_SOURCE_META).fetchall()
        audit = connection.execute(_SQL_STATUS_READ_SELECT_AUDIT).fetchall()
        disposition = connection.execute(_COUNT_SQL['disposition_audit']).fetchone()
        adoptions = connection.execute(_COUNT_SQL['stream_adoptions']).fetchone()
        ranges = connection.execute(_SQL_STATUS_READ_SELECT_DISCOVERY_RANGES).fetchall()
        pending = connection.execute(_SQL_SELECT_PENDING_RANGE_GROUP_COUNTS).fetchall()
        source_late = connection.execute(_COUNT_SQL['source_late_ids']).fetchone()
        late = connection.execute(_COUNT_SQL['late_ids']).fetchone()
    counts = {
        'source_meta': len(meta),
        'audit': sum(int(count) for _, count in audit),
        'disposition_audit': int(disposition[0]),
        'stream_adoptions': int(adoptions[0]),
        'discovery_ranges': sum(int(count) for _, count in ranges),
        'open_ranges': sum(int(count) for state, count in ranges if state == 'open'),
        'source_late_ids': int(source_late[0]),
        'late_ids': int(late[0]),
    }
    counts.update({f'pending:{group}': 0 for group in _GROUP_NAMES})
    counts.update({f'pending:{group}': int(count) for group, count in pending})
    return counts


def _strict_epoch(connection: sqlite3.Connection, lane: _LaneState, now: float) -> None:
    """Alternate an epoch capture with completion for a strict lane."""
    if lane.epoch_turn % 2 == 0:
        connection.execute(
            _SQL_STRICT_EPOCH_INSERT_EPOCHS,
            {
                'group': lane.group_name,
                'low': lane.next_event_id,
                'high': lane.next_event_id,
                'now': now,
                'generation': lane.generation,
            },
        )
        connection.execute(
            _SQL_STRICT_EPOCH_UPDATE_GROUP_STATE,
            {'group': lane.group_name, 'now': now},
        )
    else:
        connection.execute(
            _SQL_STRICT_EPOCH_UPDATE_EPOCHS,
            {'group': lane.group_name},
        )
        connection.execute(
            _SQL_STRICT_EPOCH_DELETE_EPOCHS,
            {'group': lane.group_name},
        )
    lane.epoch_turn += 1


def _strict_reader_clock(connection: sqlite3.Connection, lane: _LaneState, now: float) -> None:
    """Publish one strict reader-clock checkpoint."""
    connection.execute(
        _SQL_STRICT_READER_CLOCK_UPDATE_GROUP_STATE,
        {'group': lane.group_name, 'now': now},
    )


def _lane_arrival(connection: sqlite3.Connection, lane: _LaneState, now: float) -> str:
    """Choose and execute the highest-priority lane transaction class."""
    lane.arrivals += 1
    if lane.scan_work:
        work = lane.scan_work[0]
        if work.late_ids:
            event_id = work.late_ids[0]
            _lane_late_mark(connection, lane, work, event_id)
            work.late_ids.pop(0)
            return 'late-mark'
        _lane_scan_complete(connection, lane, work)
        lane.scan_work.pop(0)
        return 'scan-complete'
    if lane.strict and lane.arrivals % _STRICT_EPOCH_EVERY == 0:
        _strict_epoch(connection, lane, now)
        return 'epoch'
    if lane.strict and lane.arrivals % _STRICT_CLOCK_EVERY == 0:
        _strict_reader_clock(connection, lane, now)
        return 'reader-clock'
    if lane.in_flight is None:
        _lane_mark(connection, lane)
        return 'mark'
    _lane_cursor(connection, lane)
    return 'cursor'


def _lane_guarded_arrival(connection: sqlite3.Connection, lane: _LaneState, now: float) -> str:
    """Validate prepared delivery and scan state inside the write transaction."""
    generation = connection.execute(
        _SQL_SELECT_GROUP_GENERATION, {'group': lane.group_name}
    ).fetchone()[0]
    if generation != lane.generation:
        raise _LaneChangedError
    return _lane_arrival(connection, lane, now)


def _summary_metadata(run: _CellRun) -> dict[str, Any]:
    """Record environment and deterministic model constants for later evaluation."""
    result: dict[str, Any] = {
        'environment': {
            'platform': platform.platform(),
            'release': platform.release(),
            'machine': platform.machine(),
            'python': platform.python_version(),
            'sqlite': sqlite3.sqlite_version,
            'cpu_count': os.cpu_count(),
            'device_class': run.options.device_class,
            'filesystem': run.options.filesystem,
            'linux_filesystem_type': _linux_filesystem_type(run.options.work_dir),
            'commit': run.options.commit,
        },
        'cell': asdict(run.cell),
        'model': {
            'strict_groups': _STRICT_GROUPS,
            'catchup_groups': _CATCHUP_GROUPS,
            'pinned_groups': _PINNED_GROUPS,
            'capacity_group': _CAPACITY_GROUP,
            'event_rate': _EVENT_RATE,
            'lane_rate': _SCENARIOS[run.cell.scenario].lane_rate,
            'poller_rate': _SCENARIOS[run.cell.scenario].poller_rate,
            'gap_probability': _GAP_PROBABILITY,
            'merge_probability': _MERGE_PROBABILITY,
            'late_id_probability': _LATE_ID_PROBABILITY,
            'range_lifetime_secs': _RANGE_LIFETIME_SECS,
            'health_interval_secs': _HEALTH_INTERVAL_SECS,
            'restore_interval_secs': _RESTORE_INTERVAL_SECS,
            'capacity_replay_every': _CAPACITY_REPLAY_EVERY,
            'restore_chunk_ranges': _RESTORE_CHUNK_RANGES,
            'replay_distance': _REPLAY_DISTANCE,
            'snapshot_blob_bytes': _SNAPSHOT_BLOB_BYTES,
            'strict_epoch_every': _STRICT_EPOCH_EVERY,
            'strict_clock_every': _STRICT_CLOCK_EVERY,
            'seed': run.options.seed,
        },
        'population': run.population,
        'gate_eligible': False,
        'repetition': run.options.repetition,
        'start_at': run.start_at,
        'end_at': run.end_at,
        'duration_secs': run.end_at - run.start_at,
        'duration_override': run.options.duration_secs is not None,
        'min_samples': run.options.min_samples,
        'checkpoint_interval_secs': run.options.checkpoint_interval_secs,
    }
    if run.cell.scenario == 'catchup':
        result['catchup_burst_secs'] = sum(
            end - start
            for start, end, rate in _SCENARIOS['catchup'].segments(
                _CATCHUP_GROUPS[0], run.end_at - run.start_at
            )
            if rate > _SCENARIOS['catchup'].lane_rate
        )
    return result


def _take_range_hint(fd: int) -> bool:
    """Coalesce queued notifications without making durable refresh depend on them."""
    try:
        return bool(os.read(fd, 65536))
    except BlockingIOError:
        return False


def _lane_wait(
    runtime: _Runtime,
    connection: sqlite3.Connection,
    lane: _LaneState,
    hint_fd: int,
    scheduled: float,
) -> bool:
    """Service idle coordination ticks and hints without changing offered arrivals."""
    while not runtime.stop.is_set():
        now = time.monotonic()
        if now >= scheduled:
            return True
        _lane_prepare(connection, lane, now=now, hinted=_take_range_hint(hint_fd))
        delay = max(0.0, min(scheduled, lane.next_coordination_at) - time.monotonic())
        select.select([hint_fd], [], [], min(delay, 0.1))
    return False


def _thread_entry(runtime: _Runtime, job: _ThreadJob) -> None:
    """Own a connection in its thread and publish any failure to the process."""
    with _FailureBoundary(runtime, job.name):
        try:
            _run_thread_job(runtime, job)
        finally:
            job.ready.set()


def _timed_write(
    runtime: _Runtime,
    connection: sqlite3.Connection,
    writer: _Writer,
    scheduled: float,
    record: dict[str, Any],
) -> bool:
    """Capture exposed timing only, with transaction exit as the release upper bound."""
    timing = WriteTiming()
    deadline = runtime.spec.end_at + _DRAIN_SECS
    record.update(drain_deadline=deadline, acquisition_deadline=None, level='pre-acquisition')
    try:
        if writer.before is not None:
            writer.before(connection, scheduled)
        remaining = deadline - time.monotonic()
        if remaining <= 0 or runtime.stop.is_set():
            record['outcome'] = 'stopped' if runtime.stop.is_set() else 'drain-terminated'
            return False
        budget = min(
            remaining,
            write_lock_timeout_secs(_HEALTH_INTERVAL_SECS, _SUPERVISOR_POLL_INTERVAL_SECS),
        )
        record['acquisition_deadline'] = time.monotonic() + budget
        try:
            observed = (
                cast(
                    'sqlite3.Connection',
                    _ConnectionObserver(connection, runtime.phases, record['phase_id']),
                )
                if runtime.spec.extra_timing
                else connection
            )
            with runtime.lock.transaction(
                observed, timing=timing, budget_secs=budget, stop=runtime.stop
            ):
                record['level'] = 'transaction'
                if runtime.spec.extra_timing:
                    record['ownership_observed'] = _clock_sample()
                with runtime.phases.span('body', record.get('phase_id', 0)):
                    kind = writer.perform(
                        connection,
                        runtime.spec.harness_clock_start + scheduled - runtime.spec.start_at,
                    )
        finally:
            record['release_upper_bound'] = time.monotonic()
            if not math.isnan(timing.started_at):
                record['acquisition_deadline'] = timing.started_at + budget
        record['outcome'] = 'committed' if timing.finished_at <= deadline else 'late-commit'
    except (_IndexChangedError, _LaneChangedError):
        record['outcome'] = 'rejected'
        record['level'] = 'transaction'
        raise
    except DatabaseError as exc:
        record['level'] = getattr(exc, 'level', 'unexposed')
        record['outcome'] = (
            'acquisition-expired'
            if isinstance(exc, WriteLockTimeoutError)
            else 'stopped'
            if isinstance(exc, WriteLockStoppedError)
            else 'database-error'
        )
        if _retryable_failure(runtime, timing, exc):
            return False
        raise
    finally:
        record['timing'] = {
            key: None if math.isnan(value) else value for key, value in asdict(timing).items()
        }
    if writer.maintenance:
        runtime.restore_commits += int(timing.finished_at <= deadline)
    else:
        runtime.commits.append(
            {
                'writer': writer.name,
                'class': kind,
                'scheduled_at': scheduled,
                'arrival_id': record['arrival_id'],
                **asdict(timing),
            }
        )
    if writer.after is not None:
        writer.after(connection)
    return timing.finished_at <= deadline


def _attempt_write(
    runtime: _Runtime,
    connection: sqlite3.Connection,
    writer: _Writer,
    scheduled: float,
    ordinal: int = 0,
) -> bool:
    """Keep every attempted arrival and every commit, including a late completion."""
    previous, number = runtime.writer_attempts.get(writer.name, (scheduled, 0))
    number = number + 1 if previous == scheduled else 1
    runtime.writer_attempts[writer.name] = (scheduled, number)
    identity = f'{runtime.spec.role}:{writer.name}:{ordinal}'
    with _diagnostic_span(
        runtime,
        'writer-attempt',
        writer=writer.name,
        arrival_id=identity,
        scheduled_at=scheduled,
        attempt=number,
    ) as record:
        if runtime.spec.extra_timing:
            record['phase_id'] = runtime.phases.identity()
        return _timed_write(runtime, connection, writer, scheduled, record)


def _record_integer(value: object, name: str, *, minimum: int = 0) -> int:
    """Reject booleans and noninteger record counts before arithmetic."""
    if type(value) is not int or value < minimum:
        raise ValueError(f'invalid {name}: expected integer >= {minimum}')
    return value


def _unit(seed: str, index: int) -> float:
    """Return a stable hash-derived unit value for one draw."""
    digest = hashlib.blake2b(f'{seed}:{index}'.encode(), digest_size=8).digest()
    return int.from_bytes(digest) / 2**64


def _poller_plan(poller: _PollerState, now: float) -> _PollPlan:
    """Choose range changes from the poller's cached index before a write."""
    frontier = (
        poller.frontier
        + int((poller.cycle + 1) * _EVENT_RATE / poller.poller_rate)
        - int(poller.cycle * _EVENT_RATE / poller.poller_rate)
    )
    plan = _PollPlan(frontier=frontier)
    closing = [entry for entry in poller.open_ranges if entry[3] <= now]
    for index, (obligation, _low, high, _) in enumerate(closing):
        late_ids: list[int] = []
        if _unit(poller.seed, poller.cycle * 10_000 + index + 1) < _LATE_ID_PROBABILITY:
            count = 1 + int(_unit(poller.seed, poller.cycle * 10_000 + index + 101) * 3)
            late_ids = list(range(high + 1, high + count + 1))
        plan.closed.append((obligation, late_ids))
    survivors = [entry for entry in poller.open_ranges if entry not in closing]
    if poller.hold_open:
        for index in range(len(closing)):
            obligation = f'bench-{poller.next_range + index:09d}'
            low = frontier + index * 2 + 1
            plan.opened.append((obligation, low, low + 1, now + _RANGE_LIFETIME_SECS))
    gap = _unit(poller.seed, poller.cycle) < _GAP_PROBABILITY
    if gap:
        newest = (survivors + plan.opened)[-1:]
        merge = bool(newest) and (
            poller.hold_open or _unit(poller.seed, poller.cycle + 100_000) < _MERGE_PROBABILITY
        )
        if merge:
            plan.merged = (newest[0][0], max(frontier, newest[0][2] + 1))
        elif len(survivors) + len(plan.opened) < DEFAULT_MAX_OPEN_RANGES:
            obligation = f'bench-{poller.next_range + len(plan.opened):09d}'
            plan.opened.append((obligation, frontier, frontier + 1, now + _RANGE_LIFETIME_SECS))
    return plan


def _validate_child_reads(raw: dict[str, Any]) -> None:
    """Validate read spans and status counts before reader/population summaries."""
    for read in _record_rows(raw, 'reads'):
        if not isinstance(read['kind'], str):
            raise TypeError('invalid read kind')
        for name in ('start', 'end'):
            _metric_number(read[name], Path('<records>'), name)
        if read['kind'] == 'status' and read.get('outcome') != 'failed':
            for count in _metric_mapping(read['counts'], Path('<records>'), 'counts').values():
                _record_integer(count, 'status count')


def _validate_child_rows(raw: dict[str, Any]) -> None:
    """Validate nested workload rows used before evidence reconciliation."""
    for commit in _record_rows(raw, 'commits'):
        for name in ('writer', 'class', 'arrival_id'):
            if not isinstance(commit[name], str):
                raise TypeError(f'invalid commit {name}')
        for name in (
            'scheduled_at',
            'started_at',
            'local_acquired_at',
            'file_acquired_at',
            'begun_at',
            'finished_at',
        ):
            _metric_number(commit[name], Path('<records>'), name)
    for arrival in _record_rows(raw, 'offered_arrivals'):
        if not isinstance(arrival['arrival_id'], str):
            raise TypeError('invalid arrival identity')
        _metric_number(arrival['scheduled_at'], Path('<records>'), 'scheduled_at')
    _validate_child_reads(raw)
    for replay in _record_rows(raw, 'capacity_replays'):
        for name in ('finished_at', 'scheduled_at', 'late_ids', 'pending'):
            _metric_number(replay[name], Path('<records>'), name)


def _validate_clock_sample(value: object) -> dict[str, Any]:
    """Require finite nonnegative values from each separately named clock."""
    sample = _metric_mapping(value, Path('<diagnostics>'), 'clock sample')
    for key in ('monotonic', 'process_cpu', 'thread_cpu'):
        _metric_number(sample[key], Path('<diagnostics>'), key)
    return sample


def _validate_diagnostic_outcome(record: dict[str, Any]) -> None:
    """Require producer outcome names even when extra timing was disabled."""
    if record['kind'] in ('gc', 'process-sample'):
        return
    outcomes = (
        (
            'committed',
            'late-commit',
            'rejected',
            'acquisition-expired',
            'stopped',
            'drain-terminated',
            'database-error',
            'failed',
        )
        if record['kind'] == 'writer-attempt'
        else ('completed', 'failed')
    )
    if record['outcome'] not in outcomes:
        raise ValueError('invalid diagnostic outcome')


def _validate_diagnostic_record(record: dict[str, Any], role: str, *, enabled: bool) -> None:
    """Check identities and exposed timing boundaries without inferring hidden phases."""
    kind = record['kind']
    if kind not in ('process', 'writer-attempt', 'read', 'checkpoint', 'gc', 'process-sample'):
        raise ValueError('unknown diagnostic kind')
    if record['role'] != role or not isinstance(record['thread_name'], str):
        raise ValueError('invalid diagnostic identity')
    for key in ('pid', 'thread_id'):
        _record_integer(record[key], key, minimum=1)
    _validate_diagnostic_outcome(record)
    if not enabled:
        if kind != 'writer-attempt' or record['outcome'] == 'committed':
            raise ValueError('unexpected disabled-timing record')
        return
    if kind in ('gc', 'process-sample'):
        _validate_clock_sample(record)
        if kind == 'gc' and (
            record['phase'] not in ('start', 'stop')
            or _record_integer(record['generation'], 'GC generation') not in (0, 1, 2)
        ):
            raise ValueError('invalid GC record')
        return
    start = _validate_clock_sample(record['start'])
    end = _validate_clock_sample(record['end'])
    if any(end[key] < start[key] for key in ('monotonic', 'process_cpu', 'thread_cpu')):
        raise ValueError('reversed diagnostic span')


def _diagnostic_records(spec: dict[str, Any], raw: dict[str, Any]) -> list[dict[str, Any]]:
    """Validate diagnostic configuration and bounded storage independently of flags."""
    if type(spec['extra_timing']) is not bool or raw['extra_timing'] is not spec['extra_timing']:
        raise ValueError('inconsistent diagnostic timing configuration')
    cap = _record_integer(spec['diagnostic_cap'], 'spec diagnostic cap', minimum=1)
    if _record_integer(raw['diagnostic_cap'], 'raw diagnostic cap', minimum=1) != cap:
        raise ValueError('inconsistent diagnostic cap')
    _record_integer(raw['diagnostic_overflow'], 'diagnostic overflow')
    if type(raw['evidence_complete']) is not bool:
        raise ValueError('invalid raw evidence completeness')
    _record_strings(raw['evidence_errors'], 'evidence_errors')
    records = _record_rows(raw, 'diagnostics')
    if len(records) > cap:
        raise ValueError('diagnostic record count exceeds cap')
    for record in records:
        _validate_diagnostic_record(record, raw['role'], enabled=spec['extra_timing'])
    return records


def _validate_diagnostic_evidence(
    spec: dict[str, Any],
    raw: dict[str, Any],
    *,
    required: bool,
) -> None:
    """Reject truncated, contradictory or absent required timing in retained evidence."""
    records = _diagnostic_records(spec, raw)
    if (
        raw['diagnostic_overflow']
        or raw['evidence_complete'] is not True
        or raw['evidence_errors']
    ):
        raise ValueError('incomplete raw diagnostic evidence')
    if required and not spec['extra_timing']:
        raise ValueError(f'cell {_DIAGNOSTIC_CELL} requires diagnostic timing')
    if spec['extra_timing'] and not any(record['kind'] == 'process' for record in records):
        raise ValueError('missing process timing span')


def _validate_external_capacity(result: dict[str, Any], path: Path) -> None:
    """Validate cap-shape and replay values consumed by eligibility checks."""
    if result['cell']['population'] != 'cap-stress':
        return
    shape = _metric_mapping(result.get('population_during_run'), path, 'population_during_run')
    for name in _CAPACITY_MINIMUMS:
        values = _metric_mapping(shape.get(name), path, f'population_during_run.{name}')
        _metric_number(values.get('min'), path, f'population_during_run.{name}.min')
    replay = _metric_mapping(result.get('capacity_replays'), path, 'capacity_replays')
    for key in ('count', 'min_late_ids_before', 'min_pending_before'):
        _metric_number(replay.get(key), path, f'capacity_replays.{key}')
    _metric_number(result.get('duration_secs'), path, 'duration_secs')


def _validate_numeric_mapping(value: object, path: Path, field: str) -> dict[str, Any]:
    """Validate every numeric member before worst-of aggregation."""
    mapping = _metric_mapping(value, path, field)
    for key, number in mapping.items():
        _metric_number(number, path, f'{field}.{key}')
    return mapping


def _validate_external_latencies(result: dict[str, Any], path: Path) -> None:
    """Validate required gate percentiles and optional aggregated latency metrics."""
    required = {'commit_latency_ms': ('p99', 'p999'), 'all_commit_latency_ms': ('max',)}
    if result['cell']['scenario'] == 'lifecycle':
        required['full_lifecycle_latency_ms'] = ('p99',)
    for name, keys in required.items():
        value = _metric_mapping(result.get(name), path, name)
        for key in keys:
            _metric_number(value.get(key), path, f'{name}.{key}')
    for name in _AGGREGATED_LATENCY_METRICS:
        if result.get(name) is not None:
            _validate_numeric_mapping(result[name], path, name)
    for kind, value in _metric_mapping(
        result.get('commit_latency_ms_by_class', {}), path, 'commit_latency_ms_by_class'
    ).items():
        _validate_numeric_mapping(value, path, f'commit_latency_ms_by_class.{kind}')


def _validate_external_metrics(result: dict[str, Any], path: Path) -> None:
    """Validate nested values used by gate eligibility and repeat aggregation."""
    for name in (
        'start_at',
        'end_at',
        'achieved_ratio',
        'backlog_at_end',
        'busy_timeouts',
        'peak_wal_bytes_in_use',
        'peak_wal_file_bytes',
    ):
        _metric_number(result.get(name), path, name)
    _validate_numeric_mapping(result.get('expired_attempts'), path, 'expired_attempts')
    checkpoints = result.get('checkpointers')
    if not isinstance(checkpoints, list):
        raise _InvalidResultError(f'{path}: invalid result checkpointers: expected list')
    for index, item in enumerate(cast('list[Any]', checkpoints)):
        field = f'checkpointers[{index}]'
        checkpoint = _metric_mapping(item, path, field)
        _metric_number(checkpoint.get('longest_uncleared_intervals'), path, field)
        _metric_number(checkpoint.get('host_started_at'), path, field + '.host_started_at')
        _metric_number(checkpoint.get('host_finished_at'), path, field + '.host_finished_at')
        _metric_number(checkpoint.get('bound_crossings'), path, field + '.bound_crossings')
        if 'halt' not in checkpoint or not isinstance(checkpoint['halt'], (str, type(None))):
            raise ValueError(f'{path}: invalid result {field}.halt')
    _validate_external_latencies(result, path)
    _validate_external_capacity(result, path)


def _validate_external_result(result: object, path: Path) -> None:
    """Reject missing result identity and gate fields at the JSON boundary."""
    if not isinstance(result, dict) or 'cell' not in result:
        raise ValueError(f'{path}: missing result cell')
    data = cast('dict[str, Any]', result)
    cell_value = data.get('cell')
    if not isinstance(cell_value, dict) or 'number' not in cell_value:
        raise ValueError(f'{path}: invalid result cell')
    cell = cast('dict[str, Any]', cell_value)
    number = cell.get('number')
    if type(number) is not int or number not in range(1, len(_CELLS) + 1):
        raise ValueError(f'{path}: invalid result cell')
    if cell != asdict(_CELLS[number - 1]):
        raise ValueError(f'{path}: result cell does not match the gate table')
    repetition = data.get('repetition')
    if type(repetition) is not int or repetition not in range(1, _MAX_REPEAT_RUNS + 1):
        raise ValueError(f'{path}: invalid result repetition')
    if type(data.get('gate_eligible')) is not bool:
        raise ValueError(f'{path}: missing result gate_eligible')
    processes = data.get('processes')
    if not isinstance(processes, list) or any(
        not isinstance(process, dict)
        or type(cast('dict[str, Any]', process).get('exit_status')) is not int
        for process in cast('list[Any]', processes)
    ):
        raise ValueError(f'{path}: invalid result processes')
    if not data['gate_eligible'] or not cell['gated']:
        return
    _validate_external_metrics(data, path)


def _read_result_files(paths: list[Path]) -> list[dict[str, Any]]:
    """Read JSON result files, expanding a results directory by cell name."""
    files = [
        file
        for path in paths
        for file in (sorted(path.glob('cell-*.json')) if path.is_dir() else [path])
    ]
    if not files:
        raise ValueError('no cell result files found')
    resolved = [file.resolve() for file in files]
    if len(set(resolved)) != len(resolved):
        raise ValueError('duplicate result file path')
    results = [json.loads(file.read_text()) for file in files]
    for path, result in zip(files, results, strict=True):
        _validate_external_result(result, path)
        manifest = result.get('record_manifest')
        for item in cast('list[Any]', manifest) if isinstance(manifest, list) else []:
            if isinstance(item, dict):
                entry = cast('dict[str, Any]', item)
                for key in ('spec_path', 'raw_path'):
                    if isinstance(entry.get(key), str):
                        entry[key] = str(path.parent / entry[key])
    return results


def _validate_repeat_sections(identity: dict[str, Any]) -> None:
    """Require all recorded provenance and workload fields in repeat sets."""
    required = {
        'environment': (
            'platform',
            'release',
            'machine',
            'python',
            'sqlite',
            'cpu_count',
            'device_class',
            'filesystem',
            'linux_filesystem_type',
            'commit',
        ),
        'model': (
            'strict_groups',
            'catchup_groups',
            'pinned_groups',
            'capacity_group',
            'event_rate',
            'lane_rate',
            'poller_rate',
            'gap_probability',
            'merge_probability',
            'late_id_probability',
            'range_lifetime_secs',
            'health_interval_secs',
            'restore_interval_secs',
            'capacity_replay_every',
            'restore_chunk_ranges',
            'replay_distance',
            'snapshot_blob_bytes',
            'strict_epoch_every',
            'strict_clock_every',
            'seed',
        ),
        'population': (
            'population',
            'row_counts',
            'harness_clock',
            'warmup_secs',
            'gate_eligible',
        ),
    }
    for name, keys in required.items():
        missing = [key for key in keys if key not in identity[name]]
        if missing:
            raise ValueError(f'missing repeat identity {name}: {", ".join(missing)}')


def _validate_repetitions(results: list[dict[str, Any]]) -> None:
    """Require distinct numbered runs before a cell can reach a final verdict."""
    if len(results) > _MAX_REPEAT_RUNS:
        raise ValueError(f'at most {_MAX_REPEAT_RUNS} results per cell')
    identities = [_repeat_identity(result) for result in results]
    if len(identities) > 1:
        for identity in identities:
            _validate_repeat_sections(identity)
    for identity in identities[1:]:
        different = [field for field in identities[0] if identity[field] != identities[0][field]]
        if different:
            raise ValueError(f'incompatible repeat identity: {", ".join(different)}')
    repetitions = [result.get('repetition') for result in results]
    if any(
        type(number) is not int or number not in range(1, _MAX_REPEAT_RUNS + 1)
        for number in repetitions
    ):
        raise ValueError(f'result repetitions must be numbered 1 through {_MAX_REPEAT_RUNS}')
    if len(set(repetitions)) != len(repetitions):
        raise ValueError('duplicate result repetition for one cell')
    if len(results) == _MAX_REPEAT_RUNS and set(repetitions) != set(
        range(1, _MAX_REPEAT_RUNS + 1)
    ):
        raise ValueError(
            f'{_MAX_REPEAT_RUNS} results require repetitions 1 through {_MAX_REPEAT_RUNS}'
        )


def _validated_checkpoint_records(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """Validate retained checkpoint fields before reconstructing eligibility and WAL metrics."""
    attempts = _metric_list(raw['checkpoints'], Path('<evidence>'), 'checkpoints')
    records: list[dict[str, Any]] = []
    previous = 0.0
    for value in attempts:
        attempt = _metric_mapping(value, Path('<evidence>'), 'checkpoint')
        for name in ('finished_at', 'wal_file_bytes', 'frame_bytes'):
            _metric_number(attempt[name], Path('<evidence>'), f'checkpoint.{name}')
        frames = _metric_list(attempt['result'], Path('<evidence>'), 'checkpoint.result')
        if len(frames) != _CHECKPOINT_RESULT_FIELDS or any(
            type(number) is not int or number < -1 for number in frames
        ):
            raise ValueError('invalid checkpoint result')
        if attempt['outcome'] not in {
            'complete',
            'reset-observed',
            'contended',
            'partial-no-progress',
            'partial-progress',
        } or attempt['reuse'] not in {'reused', 'reuse-busy'}:
            raise ValueError('invalid checkpoint outcome')
        if attempt['finished_at'] < previous or attempt['frame_bytes'] <= 0:
            raise ValueError('invalid checkpoint ordering or frame size')
        previous = attempt['finished_at']
        records.append(attempt)
    return records


def _reconcile_checkpoint_evidence(
    result: dict[str, Any], records: list[tuple[dict[str, Any], dict[str, Any]]]
) -> None:
    """Recompute the owner's checkpoint summary and WAL peaks from retained observations."""
    interval = result['checkpoint_interval_secs']
    _metric_number(interval, Path('<evidence>'), 'checkpoint_interval_secs')
    if interval <= 0 or len(result['checkpointers']) != 1:
        raise ValueError('invalid checkpoint interval or owner summary')
    summary = result['checkpointers'][0]
    for spec, raw in records:
        owner = raw['role'] == 'site'
        if raw['checkpoint_owner'] is not owner:
            raise ValueError('invalid checkpoint ownership')
        attempts = _validated_checkpoint_records(raw)
        if not owner:
            if attempts:
                raise ValueError('unexpected non-owner checkpoint records')
            continue
        if not attempts or type(summary['attempts']) is not int:
            raise ValueError('missing checkpoints or invalid checkpoint attempt count')
        expected = _checkpoint_summary(
            attempts, spec['start_at'], min(spec['end_at'], raw['finished_at']), interval
        )
        if any(summary.get(key) != value for key, value in expected.items()):
            raise ValueError('checkpoint summary does not reconcile')
        if (
            summary.get('host_started_at') != raw['checkpoint_started_at']
            or summary.get('host_finished_at') != raw['checkpoint_finished_at']
            or summary.get('halt') != raw['checkpoint_halt']
        ):
            raise ValueError('checkpoint host lifespan does not reconcile')
        peaks = {
            'peak_wal_file_bytes': max(attempt['wal_file_bytes'] for attempt in attempts),
            'peak_wal_bytes_in_use': max(
                (
                    attempt['result'][1] * attempt['frame_bytes']
                    for attempt in attempts
                    if attempt['outcome'] != 'contended'
                ),
                default=0,
            ),
        }
        if any(result[key] != value for key, value in peaks.items()):
            raise ValueError('checkpoint WAL summary does not reconcile')


def _summarize(run: _CellRun, records: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate every process's samples without excluding offered drain arrivals."""
    commits = [
        commit
        for record in records
        for commit in record['commits']
        if commit['finished_at'] <= run.end_at + _DRAIN_SECS
    ]
    reads = [read for record in records for read in record['reads']]
    checkpoints = [attempt for record in records for attempt in record['checkpoints']]
    ordinary = [commit for commit in commits if commit['class'] != 'lifecycle']
    timeouts: Counter[str] = Counter()
    for record in records:
        timeouts.update(record['expired_attempts'])
    result = _summary_metadata(run)
    result.update(
        {
            'drain_secs': min(
                _DRAIN_SECS,
                max(
                    0.0,
                    max((record['finished_at'] for record in records), default=run.end_at)
                    - run.end_at,
                ),
            ),
            'commit_latency_ms': _metric(ordinary, 'finished_at', 'scheduled_at'),
            'all_commit_latency_ms': _metric(commits, 'finished_at', 'scheduled_at'),
            'commit_latency_ms_by_class': {
                kind: _metric(
                    [record for record in commits if record['class'] == kind],
                    'finished_at',
                    'scheduled_at',
                )
                for kind in sorted({record['class'] for record in commits})
            },
            'expired_attempts': dict(timeouts),
            'busy_timeouts': sum(record['busy_timeouts'] for record in records),
            'index_reloads': sum(record['index_reloads'] for record in records),
            'full_lifecycle_latency_ms': _metric(
                [record for record in commits if record['class'] == 'lifecycle'],
                'finished_at',
                'scheduled_at',
            )
            if run.cell.scenario == 'lifecycle'
            else None,
        }
    )
    for key, finish, start in [
        ('acquisition_ms', 'begun_at', 'started_at'),
        ('service_ms', 'finished_at', 'begun_at'),
        ('process_local_wait_ms', 'local_acquired_at', 'started_at'),
        ('file_wait_ms', 'file_acquired_at', 'local_acquired_at'),
        ('sqlite_wait_ms', 'begun_at', 'file_acquired_at'),
    ]:
        result[key] = _metric(commits, finish, start)
    result.update(
        {
            'checkpointers': [
                {
                    'role': record['role'],
                    **_checkpoint_summary(
                        record['checkpoints'],
                        run.start_at,
                        min(run.end_at, record['finished_at']),
                        run.options.checkpoint_interval_secs,
                    ),
                    'halt': record['checkpoint_halt'],
                    'host_started_at': record['checkpoint_started_at'],
                    'host_finished_at': record['checkpoint_finished_at'],
                    'bound_crossings': record['checkpoint_crossings'],
                }
                for record in records
                if record['checkpoint_owner']
            ],
            'peak_wal_bytes_in_use': max(
                (
                    attempt['result'][1] * attempt['frame_bytes']
                    for attempt in checkpoints
                    if attempt['outcome'] != 'contended'
                ),
                default=0,
            ),
            'peak_wal_file_bytes': max(
                (attempt['wal_file_bytes'] for attempt in checkpoints), default=0
            ),
            'readers': _reader_summary(reads),
            'population_during_run': _population_summary(reads),
            'capacity_replays': _capacity_summary(run.cell, records),
            'processes': [
                {
                    key: record[key]
                    for key in (
                        'role',
                        'lanes',
                        'exit_status',
                        'peak_rss_bytes',
                        'peak_open_fds',
                        'errors',
                        'health_projection_reads',
                    )
                }
                for record in records
            ],
        }
    )
    result['arrival_accounting'] = _arrival_accounting(records, run.end_at)
    drain = result['arrival_accounting']['drain_end']
    result.update(
        offered_commits=drain['offered'],
        achieved_commits=drain['completed'],
        backlog_at_end=drain['outstanding'],
        achieved_ratio=drain['completed'] / drain['offered'] if drain['offered'] else 0.0,
    )
    result.update(_retention_summary(run, records))
    if result['evidence_complete']:
        try:
            directory = cast(
                'Path', run.directory
            )  # A complete manifest has a retained directory.
            collected = [
                (json.loads(json.dumps(asdict(spec))), raw)
                for spec, raw in zip(_cell_specs(run, directory), records, strict=True)
            ]
            for spec, raw in collected:
                _validate_diagnostic_evidence(spec, raw, required=False)
            _reconcile_arrival_evidence(result, collected)
            _reconcile_checkpoint_evidence(result, collected)
        except (KeyError, TypeError, ValueError, OSError) as exc:
            result['evidence_complete'] = False
            result['evidence_errors'].append(f'reconciliation: {exc}')
    result['gate_eligible'] = _result_eligible(result) and result['evidence_complete']
    return result


def _validate_child_record(spec: _ProcessSpec, raw: dict[str, Any]) -> None:
    """Check every field consumed by aggregation before admitting a child record."""
    if raw['role'] != spec.role or raw['lanes'] != list(spec.lanes):
        raise ValueError('child identity mismatch')
    for name in (
        'exit_status',
        'offered',
        'busy_timeouts',
        'checkpoint_crossings',
        'index_reloads',
        'restore_commits',
        'peak_rss_bytes',
        'peak_open_fds',
        'health_projection_reads',
    ):
        _record_integer(raw[name], name)
    _record_strings(raw['errors'], 'errors')
    _metric_number(raw['finished_at'], Path('<records>'), 'finished_at')
    if raw['checkpoint_owner'] is not spec.poller or not isinstance(
        raw['checkpoint_halt'], str | type(None)
    ):
        raise ValueError('invalid child checkpoint ownership or halt')
    for count in _metric_mapping(
        raw['expired_attempts'], Path('<records>'), 'expired_attempts'
    ).values():
        _record_integer(count, 'expired_attempts')
    _validate_child_rows(raw)
    _validated_checkpoint_records(raw)
    _diagnostic_records(asdict(spec), raw)


def _child_result(spec: _ProcessSpec, process: ChildProcess) -> dict[str, Any]:
    """Wait for a child and retain missing output as an explicit process failure."""
    status = process.wait()
    path = Path(spec.output_path)
    try:
        result = _metric_mapping(json.loads(path.read_text()), path, 'raw')
        _validate_child_record(spec, result)
        result['exit_status'] = status or result['exit_status']
    except (OSError, ValueError, KeyError, TypeError) as exc:
        failure = f'child exited {status} without usable records: {exc}'
    else:
        return result
    runtime = _Runtime(spec, _BusyOnlyLock(timeout_secs=1))
    runtime.errors.append(failure)
    runtime.finished_at = time.monotonic()
    return _process_records(runtime)


def _validate_qualification_evidence(result: dict[str, Any]) -> None:
    """Check the retained measurement, process safety, and arrival accounting contract."""
    if result['evidence_complete'] is not True or result['evidence_errors'] != []:
        raise ValueError('incomplete evidence')
    records = _retained_process_records(result)
    processes = result['processes']
    expected_roles = {raw['role'] for _, raw in records}
    _reconcile_checkpoint_evidence(result, records)
    if (
        len(processes) != len(records)
        or {process['role'] for process in processes} != expected_roles
    ):
        raise ValueError('incomplete process summary')
    for process in [*processes, *(raw for _, raw in records)]:
        if (
            type(process['exit_status']) is not int
            or process['exit_status']
            or process['errors'] != []
        ):
            raise ValueError('process execution failure')
    for spec, raw in records:
        _validate_diagnostic_evidence(
            spec, raw, required=result['cell']['number'] == _DIAGNOSTIC_CELL
        )
        if any(spec[key] != result['cell'][key] for key in ('scenario', 'synchronous')):
            raise ValueError('process specification does not match cell')
        if (
            raw['busy_timeouts'] != 0
            or raw['checkpoint_crossings'] != 0
            or raw['checkpoint_halt'] is not None
        ):
            raise ValueError('retained process safety failure')
    if (
        len(result['checkpointers']) != 1
        or result['checkpointers'][0]['role'] != 'site'
        or not _result_eligible(result)
    ):
        raise ValueError('measurement eligibility is not established')
    _reconcile_arrival_evidence(result, records)


def _qualification_evidence_errors(result: dict[str, Any]) -> list[str]:
    """Turn missing or invalid retained evidence into an explicit safety failure."""
    try:
        _validate_qualification_evidence(result)
    except (KeyError, TypeError, ValueError, OSError) as error:
        return [f'qualification evidence: {error}']
    return []


def _wait_until(arrival: float, stop: threading.Event) -> bool:
    """Wait only for future arrivals and return whether execution may continue."""
    return not stop.wait(max(0.0, arrival - time.monotonic()))


def _checkpoint_loop(runtime: _Runtime, connection: sqlite3.Connection) -> None:
    """Register early enough for the production host's first attempt to align with the cell."""
    spec = runtime.spec
    register_at = spec.start_at - spec.checkpoint_interval_secs
    if time.monotonic() >= register_at:
        raise ValueError(
            f'checkpoint interval {spec.checkpoint_interval_secs}s leaves no registration lead'
        )
    if not _wait_until(register_at, runtime.stop):
        return
    frame_bytes = int(connection.execute('PRAGMA page_size').fetchone()[0]) + 24

    def on_failure(reason: str) -> None:
        runtime.checkpoint_halt = reason
        runtime.errors.append(reason)
        runtime.stop.set()

    with closing(
        CheckpointHost(
            interval_secs=spec.checkpoint_interval_secs,
            synchronous=spec.synchronous,
            on_failure=on_failure,
            logger=logging.getLogger('sg_event_daemon.source'),
            checkpointer_factory=_checkpoint_factory(runtime, frame_bytes),
            wal_trigger_bytes=runtime.wal_trigger_bytes,
        )
    ) as host:
        host.register(
            Path(spec.database_path),
            source_identity('bench', 'source'),
            SOURCE_SCHEMA,
            log_extra={},
        )
        _wait_until(spec.end_at, runtime.stop)


def _resource_sampler(runtime: _Runtime, _connection: sqlite3.Connection) -> None:
    """Sample this process's peak resident memory and descriptor count each second."""
    spec = runtime.spec
    for scheduled in _arrival_times(
        [(0, spec.end_at - spec.start_at, 1)], spec.start_at, spec.end_at
    ):
        if not _wait_until(scheduled, runtime.stop):
            break
        if spec.extra_timing:
            _record_diagnostic(runtime, {'kind': 'process-sample', **_clock_sample()})
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        runtime.samples.append(
            (
                int(rss if sys.platform == 'darwin' else rss * _RSS_KIB),
                len(list(Path('/dev/fd').iterdir())),
            )
        )


def _restore_capacity(runtime: _Runtime, connection: sqlite3.Connection) -> None:
    """Rebuild the removed capacity shape in paced, retryable maintenance chunks."""
    while time.monotonic() < runtime.spec.end_at and not runtime.stop.is_set():
        if not runtime.capacity.restore.wait(
            timeout=min(_RESTORE_INTERVAL_SECS, max(0, runtime.spec.end_at - time.monotonic()))
        ):
            continue
        generation = int(
            connection.execute(
                _SQL_SELECT_GROUP_GENERATION,
                {'group': _CAPACITY_GROUP},
            ).fetchone()[0]
        )
        tick = time.monotonic()
        for first, last in _capacity_chunks():

            def perform(
                conn: sqlite3.Connection,
                _now: float,
                low: str = first,
                high: str = last,
                restored_generation: int = generation,
            ) -> str:
                _capacity_restore(conn, _CAPACITY_GROUP, restored_generation, low, high)
                return 'restore'

            writer = _Writer('restore', (), perform, maintenance=True)
            while _wait_until(tick, runtime.stop) and tick < runtime.spec.end_at:
                tick += _RESTORE_INTERVAL_SECS
                if _attempt_write(runtime, connection, writer, tick - _RESTORE_INTERVAL_SECS):
                    break
            else:
                return
        runtime.capacity.counts = _capacity_counts(connection)
        runtime.capacity.restore.clear()
        runtime.capacity.ready.set()


def _status_reader(runtime: _Runtime, connection: sqlite3.Connection) -> None:
    """Read operator status once per second on an open-loop schedule."""
    spec = runtime.spec
    for scheduled in _arrival_times(
        [(0, spec.end_at - spec.start_at, 1)], spec.start_at, spec.end_at
    ):
        if not _wait_until(scheduled, runtime.stop):
            break
        _record_read(runtime, 'status', lambda: _status_read(connection))


def _worst_of(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Combine repeat samples by retaining the worst observed gate metric."""
    if not results:
        raise ValueError('at least one result is required')
    worst = json.loads(json.dumps(results[0]))
    for result in results[1:]:
        _merge_worst_latencies(worst, result)
        for name in (
            'busy_timeouts',
            'backlog_at_end',
            'peak_wal_bytes_in_use',
            'peak_wal_file_bytes',
        ):
            if name not in worst or name not in result:
                continue
            worst[name] = max(worst[name], result[name])
        worst['achieved_ratio'] = min(worst['achieved_ratio'], result['achieved_ratio'])
        for level, count in result['expired_attempts'].items():
            worst['expired_attempts'][level] = max(worst['expired_attempts'].get(level, 0), count)
        _merge_worst_checkpointers(worst, result)
        worst['processes'].extend(result['processes'])
    return worst


def _evaluate(
    results: list[dict[str, Any]], *, qualification: str = 'all-modes'
) -> dict[str, Any]:
    """Keep raw thresholds separate from the selected qualification and repeat coverage."""
    if qualification not in {'all-modes', 'normal-m7i-flex.large'}:
        raise ValueError(f'unknown qualification: {qualification}')
    if not results:
        raise ValueError('at least one result is required')
    cell = results[0]['cell']
    if any(result['cell']['number'] != cell['number'] for result in results):
        raise ValueError('all results must be for one cell')
    _validate_repetitions(results)
    evaluation: dict[str, Any] = {
        'cell': cell['number'],
        'verdict': 'reported',
        'runs': len(results),
        'failures': [],
        'near_limits': [],
        'qualification': qualification,
        'qualification_verdict': 'reported',
        'capacity_failures': [],
        'safety_failures': [],
        'repeat_required': False,
    }
    if not cell['gated']:
        return evaluation
    process_failed = any(
        process['exit_status'] for result in results for process in result.get('processes', [])
    )
    eligible = all(_eligible_for_verdict(result) for result in results)
    if not eligible:
        verdict = 'fail' if process_failed else 'not-eligible'
        evaluation.update(
            verdict=verdict,
            qualification_verdict=verdict,
            failures=['process_exit_status'] if process_failed else [],
            safety_failures=['process_exit_status']
            if process_failed
            else ['measurement_eligibility'],
        )
        evaluation['evidence_errors'] = [
            error for result in results for error in result.get('evidence_errors', [])
        ]
        return evaluation
    scoped = qualification == 'normal-m7i-flex.large'
    evidence_errors: list[str] = []
    if scoped:
        for result in results:
            _validate_external_result(result, Path('<result>'))
            evidence_errors.extend(_qualification_evidence_errors(result))
    worst = _worst_of(results)
    failures = _hard_failures(worst)
    near = [name for name, value, limit in _limit_checks(worst) if value > limit * _REPEAT_BAND]
    first = next((result for result in results if result['repetition'] == 1), None)
    first_near = (
        [name for name, value, limit in _limit_checks(first) if value > limit * _REPEAT_BAND]
        if first is not None
        else []
    )
    capacity_only = scoped and cell['number'] in {7, 8, 11}
    capacity, safety = _qualification_failures(worst, failures, capacity_only=capacity_only)
    if evidence_errors:
        safety.append('qualification_evidence')
    binding_near = [
        name for name in first_near if not capacity_only or name == 'peak_wal_file_bytes'
    ]
    incomplete = len(results) == _REPEAT_PENDING_RUNS or first is None
    repeat_required = len(results) < _MAX_REPEAT_RUNS and (bool(binding_near) or incomplete)
    raw_verdict = (
        'fail'
        if failures
        else 'incomplete'
        if incomplete
        else 'repeat'
        if len(results) == 1 and first_near
        else 'pass'
    )
    qualified = (
        'fail'
        if safety
        else 'incomplete'
        if incomplete
        else 'repeat'
        if repeat_required
        else 'pass'
    )
    evaluation.update(
        verdict=raw_verdict,
        failures=failures,
        near_limits=near,
        qualification_verdict=qualified,
        capacity_failures=capacity,
        safety_failures=safety,
        repeat_required=repeat_required,
    )
    if evidence_errors:
        evaluation['evidence_errors'] = evidence_errors
    return evaluation


def _evaluate_files(
    paths: list[Path], *, qualification: str = 'all-modes'
) -> list[dict[str, Any]]:
    """Group result files by cell and apply the selected qualification and repeat rule."""
    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for result in _read_result_files(paths):
        groups[int(result['cell']['number'])].append(result)
    return [_evaluate(groups[number], qualification=qualification) for number in sorted(groups)]


def _write_process_records(runtime: _Runtime) -> None:
    """Write the existing JSON bytes while retaining only one decoded row at a time."""
    with Path(runtime.spec.output_path).open('w', encoding='utf-8') as stream:
        stream.write('{')
        for index, (key, value) in enumerate(_process_record_fields(runtime).items()):
            if index:
                stream.write(', ')
            stream.write(json.dumps(key) + ': ')
            if isinstance(value, (Iterator, list, tuple)):
                stream.write('[')
                for row_index, record in enumerate(cast('Iterable[object]', value)):
                    if row_index:
                        stream.write(', ')
                    stream.write(json.dumps(record))
                stream.write(']')
            else:
                stream.write(json.dumps(value))
        stream.write('}')


def _write_with_reload(
    runtime: _Runtime,
    connection: sqlite3.Connection,
    writer: _Writer,
    scheduled: float,
    ordinal: int = 0,
) -> bool:
    """Replan an optimistic index conflict while retaining its original arrival."""
    while not runtime.stop.is_set() and time.monotonic() < runtime.spec.end_at + _DRAIN_SECS:
        try:
            return _attempt_write(runtime, connection, writer, scheduled, ordinal)
        except (_IndexChangedError, _LaneChangedError) as exc:
            if writer.reload is None:
                raise
            writer.reload(connection)
            if isinstance(exc, _IndexChangedError):
                runtime.index_reloads += 1
    return False


def _execute_arrivals(runtime: _Runtime, connection: sqlite3.Connection, writer: _Writer) -> None:
    """Offer every scheduled arrival and drain through the fixed cell deadline."""
    arrivals = tuple(
        t for t in writer.arrivals if runtime.spec.start_at <= t < runtime.spec.end_at
    )
    wait: Callable[[float], bool] = writer.wait or (lambda when: _wait_until(when, runtime.stop))
    for ordinal, scheduled in enumerate(arrivals):
        if not wait(scheduled):
            break
        if time.monotonic() >= runtime.spec.end_at + _DRAIN_SECS:
            break
        identity_ordinal = (
            round(scheduled - runtime.spec.start_at) if writer.name == 'lifecycle' else ordinal
        )
        _write_with_reload(runtime, connection, writer, scheduled, identity_ordinal)


def _lane_writer(runtime: _Runtime, connection: sqlite3.Connection, group: str) -> None:
    """Run one lane's aligned schedule and implementation-cadence health reads."""
    spec = runtime.spec
    lane = _LaneState(
        group, strict=group in _STRICT_GROUPS, scan_suppressed=frozenset(spec.scan_suppressed)
    )
    _lane_resume(connection, lane)
    segments = _SCENARIOS[spec.scenario].segments(group, spec.end_at - spec.start_at)
    with _range_hint_pipe(spec.database_path, group) as hint_fd:
        writer = _Writer(
            group,
            tuple(_arrival_times(segments, spec.start_at, spec.end_at)),
            lambda conn, now: _lane_guarded_arrival(conn, lane, now),
            before=lambda conn, _scheduled: _lane_prepare(
                conn, lane, now=time.monotonic(), hinted=_take_range_hint(hint_fd)
            ),
            after=lambda conn: _lane_health(runtime, conn, lane),
            reload=lambda conn: _lane_prepare(conn, lane, now=time.monotonic(), force=True),
            wait=lambda scheduled: _lane_wait(runtime, connection, lane, hint_fd, scheduled),
        )
        _execute_arrivals(runtime, connection, writer)
        _lane_wait(runtime, connection, lane, hint_fd, spec.end_at)


def _lifecycle_writer(runtime: _Runtime, connection: sqlite3.Connection) -> None:
    """Replay at one hertz and request restoration after each capacity replay."""
    spec = runtime.spec
    state = _LifecycleState()
    for index, scheduled in enumerate(
        _arrival_times([(0, spec.end_at - spec.start_at, 1)], spec.start_at, spec.end_at)
    ):
        if not _wait_until(scheduled, runtime.stop):
            break
        target = _lifecycle_target(
            state,
            index,
            capacity_ready=runtime.capacity.ready.is_set(),
            population=spec.population,
        )
        counts = dict(runtime.capacity.counts)

        def perform(conn: sqlite3.Connection, now: float, group: str = target) -> str:
            _lifecycle_replay(conn, group, now)
            return 'lifecycle'

        before = len(runtime.commits)
        _execute_arrivals(runtime, connection, _Writer('lifecycle', (scheduled,), perform))
        replay = next(
            (record for record in runtime.commits[before:] if record['writer'] == 'lifecycle'),
            None,
        )
        if target == _CAPACITY_GROUP and replay is not None:
            runtime.capacity.ready.clear()
            runtime.capacity_replays.append({**replay, **counts})
            runtime.capacity.restore.set()


def _poller_writer(runtime: _Runtime, connection: sqlite3.Connection) -> None:
    """Plan observations outside the write lock and retry stale indexes safely."""
    spec = runtime.spec
    poller = _PollerState(seed=spec.seed, hold_open=spec.population == 'cap-stress')
    _poller_reload(connection, poller)
    plan = _PollPlan(0)
    range_changed = False

    def prepare(_connection: sqlite3.Connection, scheduled: float) -> None:
        nonlocal plan, range_changed
        range_changed = False
        plan = _poller_plan(poller, spec.harness_clock_start + scheduled - spec.start_at)

    def perform(conn: sqlite3.Connection, now: float) -> str:
        nonlocal range_changed
        range_changed = _poller_observe(conn, poller, plan, now)
        return 'poller'

    arrivals = _arrival_times(
        [(0, spec.end_at - spec.start_at, _SCENARIOS[spec.scenario].poller_rate)],
        spec.start_at,
        spec.end_at,
    )
    with ExitStack() as stack:
        hint_fds = tuple(
            stack.enter_context(_range_hint_pipe(spec.database_path, group))
            for group in _GROUP_NAMES
        )

        def committed(_connection: sqlite3.Connection) -> None:
            if range_changed:
                _publish_range_hints(hint_fds)

        writer = _Writer(
            'poller',
            tuple(arrivals),
            perform,
            before=prepare,
            after=committed,
            reload=lambda conn: _poller_reload(conn, poller),
        )
        _execute_arrivals(runtime, connection, writer)


def _thread_jobs(spec: _ProcessSpec) -> list[_ThreadJob]:
    """Create exactly one connection-owning job per workload component."""
    jobs: list[_ThreadJob] = []
    for group in spec.lanes:

        def run(runtime: _Runtime, connection: sqlite3.Connection, lane: str = group) -> None:
            _lane_writer(runtime, connection, lane)

        jobs.append(_ThreadJob(group, run, spec.synchronous))
    optional = [
        (spec.poller, 'poller', _poller_writer, spec.synchronous),
        (spec.poller, 'checkpoint', _checkpoint_loop, spec.synchronous),
        (spec.lifecycle, 'lifecycle', _lifecycle_writer, 'FULL'),
        (spec.status_reader, 'status', _status_reader, spec.synchronous),
        (
            spec.lifecycle and spec.population == 'cap-stress',
            'restore',
            _restore_capacity,
            'NORMAL',
        ),
    ]
    jobs.extend(
        _ThreadJob(name, run, synchronous)
        for enabled, name, run, synchronous in optional
        if enabled
    )
    jobs.extend((_ThreadJob('resources', _resource_sampler, spec.synchronous),))
    return jobs


def _run_threads(runtime: _Runtime) -> None:
    """Align prepared threads, stop acquisition after drain, and join all owners."""
    spec = runtime.spec
    jobs = _thread_jobs(spec)
    threads = [
        threading.Thread(target=_thread_entry, args=(runtime, job), name=job.name) for job in jobs
    ]
    try:
        for thread in threads:
            thread.start()
        for job in jobs:
            job.ready.wait()
        if time.monotonic() > spec.start_at:
            runtime.errors.append('late-start')
            runtime.stop.set()
        runtime.go.set()
        for thread in threads:
            thread.join(timeout=max(0, spec.end_at + _DRAIN_SECS - time.monotonic()))
        runtime.stop.set()
        for thread in threads:
            thread.join()
        runtime.finished_at = time.monotonic()
    finally:
        runtime.go.set()
        runtime.stop.set()


def _run_process(
    spec: _ProcessSpec, *, wal_trigger_bytes: int = WAL_TRIGGER_BYTES, persist: bool = False
) -> dict[str, Any]:
    """Run synchronized threads, optionally exporting before releasing the runtime."""
    budget = write_lock_timeout_secs(_HEALTH_INTERVAL_SECS, _SUPERVISOR_POLL_INTERVAL_SECS)
    runtime = _Runtime(
        spec, _BusyOnlyLock(timeout_secs=budget), wal_trigger_bytes=wal_trigger_bytes
    )
    with _diagnostic_session(runtime), _FailureBoundary(runtime, 'setup'):
        if spec.arbitration != 'busy-only':
            runtime.lock = SourceWriteLock(
                Path(f'{spec.database_path}.write-lock'),
                database_path=Path(spec.database_path),
                timeout_secs=budget,
                wait_phase_grace_secs=_HEALTH_INTERVAL_SECS,
            )
        _run_threads(runtime)
    runtime.finished_at = time.monotonic()
    runtime.lock.close()
    _wait_until(spec.end_at + _DRAIN_SECS, threading.Event())
    if persist:
        _write_process_records(runtime)
        return {'exit_status': int(bool(runtime.errors))}
    return _process_records(runtime)


def _child_main(argv: list[str]) -> int:
    """Run the internal child command and persist its complete process records."""
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['child'])
    parser.add_argument('--spec', type=Path, required=True)
    args = parser.parse_args(argv)
    spec = _ProcessSpec.from_json(args.spec.read_text())
    result = _run_process(spec, persist=True)
    return int(result['exit_status'])


def _run_cell(cell: _Cell, population: Path, options: _RunOptions) -> dict[str, Any]:
    """Copy a seed into an owner-only directory and execute all process roles."""
    work_dir = ensure_secure_state_dir(options.work_dir)
    directory = Path(tempfile.mkdtemp(prefix=f'cell-{cell.number:02d}-', dir=work_dir))
    shutil.copyfile(population, directory / 'source.db')
    (directory / 'source.db').chmod(0o600)
    metadata_path = Path(f'{population}.json')
    metadata = (
        json.loads(metadata_path.read_text())
        if metadata_path.exists()
        else {'population': cell.population, 'gate_eligible': False}
    )
    scenario = _SCENARIOS[cell.scenario]
    duration = (
        options.duration_secs
        if options.duration_secs is not None
        else _default_duration(scenario, options.min_samples)
    )
    start = time.monotonic() + _START_DELAY_SECS
    run = _CellRun(cell, options, start, start + duration, metadata, directory)
    specs = _cell_specs(run, directory)
    env = dict(os.environ)
    src = str(Path(__file__).resolve().parents[1] / 'src')
    env['PYTHONPATH'] = os.pathsep.join(filter(None, (src, env.get('PYTHONPATH', ''))))
    children: list[tuple[_ProcessSpec, ChildProcess]] = []
    for spec in specs:
        path = directory / f'{spec.role}-spec.json'
        path.write_text(spec.to_json())
        if spec.role != 'site':
            children.append(
                (
                    spec,
                    spawn_child(
                        [
                            sys.executable,
                            str(Path(__file__).resolve()),
                            'child',
                            '--spec',
                            str(path),
                        ],
                        env=env,
                    ),
                )
            )
    _run_process(specs[0], persist=True)
    # The compact runtime is gone before loading the shared-key JSON object graph.
    records = [json.loads(Path(specs[0].output_path).read_text())]
    records.extend(_child_result(spec, process) for spec, process in children)
    return _summarize(run, records)


def _run_command(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run one cell, save its JSON result, and return its one evaluation."""
    cell_number = int(args.cell)
    if cell_number not in range(1, len(_CELLS) + 1):
        raise ValueError(f'cell must be between 1 and {len(_CELLS)}')
    if args.duration_secs is not None and args.duration_secs <= 0:
        raise ValueError('duration seconds must be positive')
    if args.min_samples <= 0 or args.checkpoint_interval_secs <= 0 or args.repetition <= 0:
        raise ValueError('sample, checkpoint interval, and repetition must be positive')
    options = _RunOptions(
        args.work_dir,
        args.duration_secs,
        args.min_samples,
        args.checkpoint_interval_secs,
        args.repetition,
        args.device_class,
        args.filesystem,
        args.commit,
    )
    result = _run_cell(_CELLS[cell_number - 1], args.population, options)
    evaluation = _evaluate([result], qualification=args.qualification)
    result.update({key: value for key, value in evaluation.items() if key not in {'cell', 'runs'}})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    _persist_cell_result(result, args.output)
    return result, evaluation


def _seed_representative(path: Path, warmup_secs: float) -> dict[str, Any]:
    """Warm the documented single-process writers and truncate the seed WAL."""
    handle = open_database(
        path, source_identity('bench', 'source'), SOURCE_SCHEMA, synchronous='NORMAL'
    )
    try:
        handle.connection.execute(_SQL_BEGIN_IMMEDIATE)
        _seed_base(handle.connection, strict_groups=_STRICT_GROUPS)
        handle.connection.commit()
    finally:
        handle.close()
    start = time.monotonic() + _START_DELAY_SECS
    spec = _ProcessSpec(
        role='site',
        database_path=str(path),
        lanes=_GROUP_NAMES,
        poller=True,
        lifecycle=False,
        status_reader=True,
        scenario='documented',
        synchronous='NORMAL',
        arbitration='two-level',
        population='representative',
        start_at=start,
        end_at=start + warmup_secs,
        harness_clock_start=0.0,
        checkpoint_interval_secs=DEFAULT_CHECKPOINT_INTERVAL_SECS,
        seed='bench',
        scan_suppressed=(),
        output_path=str(path.with_suffix('.warmup.json')),
    )
    record = _run_process(spec)
    if record['exit_status']:
        raise RuntimeError(f'representative warm-up failed: {record["errors"]}')
    handle = open_database(
        path, source_identity('bench', 'source'), SOURCE_SCHEMA, synchronous='NORMAL'
    )
    try:
        checkpoint = handle.connection.execute(_SQL_WAL_CHECKPOINT_TRUNCATE).fetchone()
        if checkpoint is None or checkpoint[0] != 0:
            raise RuntimeError('representative WAL checkpoint was busy')
        counts = _population_counts(handle.connection)
    finally:
        handle.close()
    return {
        'population': 'representative',
        'row_counts': counts,
        'harness_clock': warmup_secs,
        'warmup_secs': warmup_secs,
        'gate_eligible': warmup_secs >= _MIN_DURATION_SECS,
    }


def _seed_command(population: str, output: Path, warmup_secs: float) -> dict[str, Any]:
    """Create a population and its adjacent eligibility metadata."""
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise ValueError(f'population already exists: {output}')
    if warmup_secs <= 0:
        raise ValueError('warmup seconds must be positive')
    if population == 'representative':
        metadata = _seed_representative(output, warmup_secs)
    else:
        _seed_cap_stress(output)
        handle = open_database(
            output, source_identity('bench', 'source'), SOURCE_SCHEMA, synchronous='NORMAL'
        )
        try:
            counts = _population_counts(handle.connection)
        finally:
            handle.close()
        metadata = {
            'population': 'cap-stress',
            'row_counts': counts,
            'harness_clock': 0.0,
            'warmup_secs': 0.0,
            'gate_eligible': all(
                counts.get(name, 0) >= minimum for name, minimum in _CAPACITY_MINIMUMS.items()
            ),
        }
    Path(f'{output}.json').write_text(json.dumps(metadata, indent=2) + '\n')
    return metadata


def _gate_command(args: argparse.Namespace, command_line: list[str]) -> list[dict[str, Any]]:
    """Seed both populations and run selected cells in specification order."""
    args.results_dir.mkdir(parents=True, exist_ok=True)
    ensure_secure_state_dir(args.work_dir)
    selected = (
        list(range(1, len(_CELLS) + 1))
        if args.cells is None
        else [int(value) for value in args.cells.split(',')]
    )
    if not selected or any(number not in range(1, len(_CELLS) + 1) for number in selected):
        raise ValueError(f'cells must be a comma-separated list from 1 to {len(_CELLS)}')
    seeds = {
        population: args.work_dir / f'{population}.db'
        for population in ('representative', 'cap-stress')
    }
    for population, path in seeds.items():
        _seed_command(population, path, args.warmup_secs)
    evaluations: list[dict[str, Any]] = []
    environment: dict[str, Any] = {}
    for cell in _CELLS:
        if cell.number not in selected:
            continue
        run_args = argparse.Namespace(
            cell=cell.number,
            population=seeds[cell.population],
            work_dir=args.work_dir,
            device_class=args.device_class,
            filesystem=args.filesystem,
            output=args.results_dir / f'cell-{cell.number:02d}.json',
            repetition=1,
            duration_secs=None,
            min_samples=_MIN_SAMPLES,
            checkpoint_interval_secs=DEFAULT_CHECKPOINT_INTERVAL_SECS,
            commit=args.commit,
            qualification=args.qualification,
        )
        result, evaluation = _run_command(run_args)
        environment = result['environment']
        evaluations.append(evaluation)
    summary = {
        'cells': evaluations,
        **_qualification_summary(evaluations),
        'environment': environment,
    }
    (args.results_dir / 'gate-summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    exact_command = shlex.join([sys.executable, 'benchmarks/storage_contention.py', *command_line])
    (args.results_dir / 'commands.txt').write_text(
        f'command: {exact_command}\n'
        f'commit: {args.commit}\npython: {platform.python_version()}\n'
        f'sqlite: {sqlite3.sqlite_version}\n'
    )
    return evaluations


def _main(argv: Sequence[str] | None = None) -> int:
    """Dispatch storage gate commands and map expected errors to exit status 2."""
    parser = argparse.ArgumentParser(description='Per-source storage contention gate')
    commands = parser.add_subparsers(dest='command', required=True)
    seed = commands.add_parser('seed')
    seed.add_argument('--population', choices=('representative', 'cap-stress'), required=True)
    seed.add_argument('--output', type=Path, required=True)
    seed.add_argument('--warmup-secs', type=float, default=_MIN_DURATION_SECS)
    run = commands.add_parser('run')
    run.add_argument('--cell', type=int, required=True)
    run.add_argument('--population', type=Path, required=True)
    run.add_argument('--work-dir', type=Path, required=True)
    run.add_argument('--device-class', required=True)
    run.add_argument('--filesystem', required=True)
    run.add_argument('--output', type=Path, required=True)
    run.add_argument('--repetition', type=int, default=1)
    run.add_argument('--duration-secs', type=float)
    run.add_argument('--min-samples', type=int, default=_MIN_SAMPLES)
    run.add_argument(
        '--checkpoint-interval-secs', type=float, default=DEFAULT_CHECKPOINT_INTERVAL_SECS
    )
    run.add_argument('--commit')
    gate = commands.add_parser('gate')
    gate.add_argument('--work-dir', type=Path, required=True)
    gate.add_argument('--results-dir', type=Path, required=True)
    gate.add_argument('--device-class', required=True)
    gate.add_argument('--filesystem', required=True)
    gate.add_argument('--cells')
    gate.add_argument('--warmup-secs', type=float, default=_MIN_DURATION_SECS)
    gate.add_argument('--commit')
    evaluate = commands.add_parser('evaluate')
    evaluate.add_argument('files', nargs='+', type=Path)
    for command in (run, gate, evaluate):
        command.add_argument(
            '--qualification', choices=('all-modes', 'normal-m7i-flex.large'), default='all-modes'
        )
    child = commands.add_parser('child')
    child.add_argument('--spec', type=Path, required=True)
    arguments = list(argv) if argv is not None else sys.argv[1:]
    try:
        args = parser.parse_args(arguments)
        if args.command == 'child':
            return _child_main(['child', '--spec', str(args.spec)])
        if args.command == 'seed':
            _seed_command(args.population, args.output, args.warmup_secs)
            return 0
        if args.command == 'run':
            evaluations = [_run_command(args)[1]]
            _print_verdicts(evaluations)
            return _evaluation_status(evaluations)
        if args.command == 'gate':
            evaluations = _gate_command(args, arguments)
        else:
            evaluations = _evaluate_files(args.files, qualification=args.qualification)
        _print_verdicts(evaluations)
        return _evaluation_status(evaluations)
    except SystemExit as error:
        return error.code if isinstance(error.code, int) else 2
    except (OSError, ValueError, DatabaseError, SecurityError) as error:
        print(f'error: {error}', file=sys.stderr)
        return 2


# Construct these values after their class; keep executable initialization
# together at the footer so definition sorting preserves this dependency.
_SCENARIOS: Mapping[str, _Scenario] = {
    'documented': _Scenario(10.0),
    'worst': _Scenario(22.0),
    'catchup': _Scenario(10.0, catchup=True),
    'lifecycle': _Scenario(22.0, lifecycle=True),
}

_CELLS: tuple[_Cell, ...] = (
    _Cell(1, 'documented', 'single', 'NORMAL', 'two-level', 'representative', gated=True),
    _Cell(2, 'documented', 'dedicated', 'NORMAL', 'two-level', 'representative', gated=True),
    _Cell(3, 'worst', 'single', 'NORMAL', 'two-level', 'representative', gated=True),
    _Cell(4, 'worst', 'dedicated', 'NORMAL', 'two-level', 'representative', gated=True),
    _Cell(5, 'catchup', 'single', 'NORMAL', 'two-level', 'representative', gated=True),
    _Cell(6, 'catchup', 'dedicated', 'NORMAL', 'two-level', 'representative', gated=True),
    _Cell(7, 'worst', 'single', 'FULL', 'two-level', 'representative', gated=True),
    _Cell(8, 'worst', 'dedicated', 'FULL', 'two-level', 'representative', gated=True),
    _Cell(9, 'worst', 'named', 'NORMAL', 'two-level', 'representative', gated=True),
    _Cell(10, 'lifecycle', 'single', 'NORMAL', 'two-level', 'cap-stress', gated=True),
    _Cell(11, 'lifecycle', 'single', 'FULL', 'two-level', 'cap-stress', gated=True),
    _Cell(12, 'worst', 'dedicated', 'NORMAL', 'busy-only', 'representative', gated=False),
)


if __name__ == '__main__':
    sys.exit(_main())
