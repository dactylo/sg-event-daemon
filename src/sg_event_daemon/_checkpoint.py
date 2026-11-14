"""Classify WAL checkpoints, reuse the WAL, and warn at runtime bounds."""

from __future__ import annotations

import math
import os
import sqlite3
import time
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeAlias, TypedDict, Unpack

from sg_event_daemon._sqlite import (
    WAL_IN_USE_LIMIT_BYTES,
    WAL_REUSE_WAIT_SECS,
    DatabaseError,
    temporary_busy_timeout,
    wal_file_bytes,
)

if TYPE_CHECKING:
    import logging
    from collections.abc import Callable, Mapping
    from pathlib import Path

_CheckpointBound: TypeAlias = Literal['incomplete-duration', 'wal-growth']
_CheckpointOutcome: TypeAlias = Literal[
    'complete', 'reset-observed', 'contended', 'partial-no-progress', 'partial-progress'
]
_CheckpointResult: TypeAlias = tuple[int, int, int]
_ReuseOutcome: TypeAlias = Literal['reused', 'reuse-busy']

_CHECKPOINT_FAILURE_INTERVALS = 5
_CLEARING_OUTCOMES: frozenset[_CheckpointOutcome] = frozenset({'complete', 'reset-observed'})
_RECENT_ATTEMPTS = 8


@dataclass(frozen=True)
class CheckpointAttempt:
    """Record a passive result, subsequent reuse result, and pre-reuse WAL size.

    `finished_at` is the monotonic time after both SQLite calls. `wal_bytes`
    includes uncommitted spill and is sampled before reuse can truncate it.
    """

    outcome: _CheckpointOutcome
    result: _CheckpointResult
    reuse: _ReuseOutcome
    reuse_result: _CheckpointResult
    wal_bytes: int
    finished_at: float


class _CheckpointerSeams(TypedDict, total=False):
    """Type test replacements for timing, SQLite calls, sync, and growth limits."""

    clock: Callable[[], float]
    run_checkpoint: Callable[[], _CheckpointResult]
    run_reuse: Callable[[], _CheckpointResult]
    wal_bytes: Callable[[], int]
    sync_wal: Callable[[], None]
    wal_limit_bytes: int


class Checkpointer:
    """Own checkpoint history and a lazily opened WAL sync descriptor.

    Use from the connection's owning thread. Each attempt runs PASSIVE then
    TRUNCATE, without taking daemon locks. Bounds warn on transitions; SQLite
    failures remain fatal. Call `close()` before closing the database
    connection.
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        logger: logging.Logger,
        log_extra: Mapping[str, str],
        **seams: Unpack[_CheckpointerSeams],
    ) -> None:
        """Configure one file's attempts and warning owner.

        Args:
            connection: Dedicated checkpoint connection.
            path: Database path used for the WAL sample and diagnostics.
            interval_secs: Finite positive schedule interval; five intervals
                without a clearing outcome cross the duration bound.
            logger: Owning source or supervisor logger.
            log_extra: Owner identity fields attached to transition messages.
            **seams: Optional replacements for tests.

        Raises:
            ValueError: The interval or WAL limit is not positive and finite.
        """
        if not math.isfinite(interval_secs) or interval_secs <= 0:
            raise ValueError('checkpoint interval must be finite and positive')
        limit = seams.get('wal_limit_bytes', WAL_IN_USE_LIMIT_BYTES)
        if limit <= 0:
            raise ValueError('WAL limit must be positive')
        self._connection = connection
        self._path = path
        self._interval_secs = interval_secs
        self._logger = logger
        self._log_extra = dict(log_extra)
        self._clock = seams.get('clock', time.monotonic)
        self._run_checkpoint = seams.get('run_checkpoint', self._passive_checkpoint)
        self._run_reuse = seams.get('run_reuse', self._reuse_checkpoint)
        self._wal_bytes = seams.get('wal_bytes', lambda: wal_file_bytes(path))
        self._sync_wal = seams.get('sync_wal', self._sync)
        self._wal_limit_bytes = limit
        self._wal_fd: int | None = None
        self._cleared_at = self._clock()
        self._previous_log: int | None = None
        self._previous_checkpointed: int | None = None
        self._recent: deque[CheckpointAttempt] = deque(maxlen=_RECENT_ATTEMPTS)
        self._crossed: set[_CheckpointBound] = set()

    def _bound(
        self, bound: _CheckpointBound, *, crossed: bool, detail: str, elapsed: float
    ) -> None:
        """Log each crossing and clearing once for this bound."""
        name = 'WAL growth' if bound == 'wal-growth' else 'incomplete duration'
        if crossed and bound not in self._crossed:
            self._crossed.add(bound)
            recent = ', '.join(
                f'{item.outcome} {item.result} {item.reuse} {item.reuse_result}'
                for item in self._recent
            )
            self._logger.warning(
                'checkpoint %s bound exceeded for %s: %s; %.1fs since the last '
                'complete, reset-observed, or reused attempt; recent attempts: %s',
                name,
                self._path,
                detail,
                elapsed,
                recent,
                extra=self._log_extra,
            )
        elif not crossed and bound in self._crossed:
            self._crossed.remove(bound)
            self._logger.info(
                'checkpoint %s bound cleared for %s', name, self._path, extra=self._log_extra
            )

    def _checkpoint(self, mode: Literal['PASSIVE', 'TRUNCATE']) -> _CheckpointResult:
        """Read one SQLite checkpoint result, preserving database failures."""
        row = self._connection.execute(f'PRAGMA wal_checkpoint({mode})').fetchone()
        if row is None:
            raise DatabaseError('SQLite returned no checkpoint result')
        busy, log_frames, checkpointed = (int(value) for value in row)
        return busy, log_frames, checkpointed

    def _classify(self, result: _CheckpointResult) -> _CheckpointOutcome:
        """Classify the passive call against the previous non-contended sample."""
        busy, log_frames, checkpointed = result
        if busy != 0:
            return 'contended'
        if checkpointed >= log_frames:
            return 'complete'
        if self._previous_log is not None and log_frames < self._previous_log:
            return 'reset-observed'
        if self._previous_checkpointed is not None and checkpointed == self._previous_checkpointed:
            return 'partial-no-progress'
        return 'partial-progress'

    def _passive_checkpoint(self) -> _CheckpointResult:
        """Run one passive call without a retry."""
        try:
            return self._checkpoint('PASSIVE')
        except sqlite3.Error as exc:
            raise DatabaseError(str(exc)) from exc

    def _reuse_checkpoint(self) -> _CheckpointResult:
        """Truncate after waiting at most the reuse budget, restoring the timeout."""
        try:
            with temporary_busy_timeout(self._connection, int(WAL_REUSE_WAIT_SECS * 1000)):
                return self._checkpoint('TRUNCATE')
        except sqlite3.Error as exc:
            raise DatabaseError(str(exc)) from exc

    def _sync(self) -> None:
        """Sync SQLite's WAL writes through one lazily opened file descriptor."""
        if self._wal_fd is None:
            try:
                self._wal_fd = os.open(f'{self._path}-wal', os.O_RDONLY)
            except FileNotFoundError:
                return
        os.fsync(self._wal_fd)

    def close(self) -> None:
        """Close the WAL sync descriptor, leaving the caller's connection open.

        Raises:
            OSError: Closing the owned WAL sync descriptor fails.
        """
        if self._wal_fd is not None:
            os.close(self._wal_fd)
            self._wal_fd = None

    @property
    def crossed(self) -> frozenset[_CheckpointBound]:
        """Return the bounds still crossed at the latest completed attempt."""
        return frozenset(self._crossed)

    def run_once(self) -> CheckpointAttempt:
        """Run passive and reuse calls, sample growth, sync if needed, and warn.

        Returns:
            Both classified outcomes and the physical WAL size before reuse.

        Raises:
            DatabaseError: A SQLite checkpoint operation failed.
            OSError: Inspecting or synchronizing the WAL failed.
        """
        result = self._run_checkpoint()
        outcome = self._classify(result)
        wal_bytes = self._wal_bytes()
        growth = wal_bytes > self._wal_limit_bytes
        reuse_result = self._run_reuse()
        reuse: _ReuseOutcome = 'reused' if reuse_result[0] == 0 else 'reuse-busy'
        attempt = CheckpointAttempt(outcome, result, reuse, reuse_result, wal_bytes, self._clock())
        self._recent.append(attempt)
        if outcome != 'contended':
            self._previous_log, self._previous_checkpointed = result[1:]
        clearing = outcome in _CLEARING_OUTCOMES or reuse == 'reused'
        if clearing:
            self._cleared_at = attempt.finished_at
        else:
            self._sync_wal()
        elapsed = attempt.finished_at - self._cleared_at
        self._bound(
            'wal-growth',
            crossed=growth,
            detail=f'WAL file is {wal_bytes} bytes, over {self._wal_limit_bytes}',
            elapsed=elapsed,
        )
        self._bound(
            'incomplete-duration',
            crossed=elapsed >= _CHECKPOINT_FAILURE_INTERVALS * self._interval_secs,
            detail=f'5 intervals of {self._interval_secs:.1f}s elapsed',
            elapsed=elapsed,
        )
        return attempt
