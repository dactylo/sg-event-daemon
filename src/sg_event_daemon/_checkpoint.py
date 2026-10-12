"""Classify passive WAL checkpoints and enforce their runtime bounds."""

from __future__ import annotations

import math
import random
import sqlite3
import time
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, TypeAlias, TypedDict, Unpack

from sg_event_daemon._sqlite import DatabaseError

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

CheckpointOutcome: TypeAlias = Literal[
    'complete',
    'reset-observed',
    'contended',
    'partial-no-progress',
    'partial-progress',
]
CheckpointResult: TypeAlias = tuple[int, int, int]
CheckpointState: TypeAlias = Literal['idle', 'complete', 'incomplete']

_WAL_IN_USE_LIMIT_BYTES = 256 * 1024 * 1024
_CHECKPOINT_FAILURE_INTERVALS = 5
_CLEARING_OUTCOMES: frozenset[CheckpointOutcome] = frozenset({'complete', 'reset-observed'})
_RECENT_ATTEMPTS = 8
_WAL_FRAME_HEADER_BYTES = 24


@dataclass(frozen=True)
class CheckpointAttempt:
    """Record one classified passive checkpoint attempt.

    Attributes:
        outcome: Classification under the checkpoint policy.
        result: The `(busy, log, checkpointed)` row of the final try, which is
            the retry's row when the first try was contended.
        retried: Whether the first try was contended and retried.
        finished_at: Monotonic time when the attempt was classified.
    """

    outcome: CheckpointOutcome
    result: CheckpointResult
    retried: bool
    finished_at: float


class CheckpointBoundError(DatabaseError):
    """Report a runtime bound violation with its classified checkpoint attempt.

    Attributes:
        attempt: The observed attempt that triggered the bound.
    """

    def __init__(self, message: str, attempt: CheckpointAttempt) -> None:
        """Retain the classified attempt alongside the diagnostic message."""
        super().__init__(message)
        self.attempt = attempt


class _CheckpointerSeams(TypedDict, total=False):
    """Type optional clock, randomness, result, sleep, and bound replacements for tests."""

    clock: Callable[[], float]
    random_value: Callable[[], float]
    run_checkpoint: Callable[[], CheckpointResult]
    sleep: Callable[[float], None]
    wal_limit_bytes: int


class Checkpointer:
    """Run one process's passive checkpoints on one database file.

    Each `run_once()` call makes one attempt; a contended first try is retried
    once after a random delay of up to half an interval. Attempts are
    classified as complete, reset observed, contended, or partial with or
    without progress.
    The checkpointer halts by raising `DatabaseError` when no complete or
    reset-observed attempt has occurred for five intervals, measured from the
    last such attempt or from construction, or when a non-contended attempt
    reports more committed WAL frames than `_WAL_IN_USE_LIMIT_BYTES` occupy.
    That limit is a tripwire on observed committed frames, not a ceiling on the
    WAL file's size. Use it only from the thread that opened its connection.
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        path: Path,
        *,
        interval_secs: float,
        **seams: Unpack[_CheckpointerSeams],
    ) -> None:
        """Prepare a checkpointer for an open WAL connection.

        Args:
            connection: Connection dedicated to checkpoints on this file.
            path: Database path named in halt messages.
            interval_secs: Scheduled time between attempts; the duration bound
                and the retry delay derive from it.
            **seams: Optional replacements for tests.

        Raises:
            ValueError: `interval_secs` is not finite and positive, or
                `wal_limit_bytes` is not positive.
            DatabaseError: SQLite cannot report the page size.
        """
        if not math.isfinite(interval_secs) or interval_secs <= 0:
            raise ValueError('checkpoint interval must be finite and positive')
        wal_limit_bytes = seams.get('wal_limit_bytes', _WAL_IN_USE_LIMIT_BYTES)
        if wal_limit_bytes <= 0:
            raise ValueError('WAL limit must be positive')
        self._connection = connection
        self._path = path
        self._interval_secs = interval_secs
        self._clock = seams.get('clock', time.monotonic)
        self._random_value = seams.get('random_value', random.random)
        self._sleep = seams.get('sleep', time.sleep)
        self._run_checkpoint = seams.get('run_checkpoint', self._passive_checkpoint)
        self._wal_limit_bytes = wal_limit_bytes
        self._frame_bytes = _page_size(connection) + _WAL_FRAME_HEADER_BYTES
        self._cleared_at = self._clock()
        self._previous_log: int | None = None
        self._previous_checkpointed: int | None = None
        self._recent: deque[CheckpointAttempt] = deque(maxlen=_RECENT_ATTEMPTS)
        self._state: CheckpointState = 'idle'

    def _classify(self, result: CheckpointResult) -> CheckpointOutcome:
        """Classify one final try against this checkpointer's previous sample."""
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

    def _halt(self, bound: str, detail: str) -> CheckpointBoundError:
        """Build the halt error naming the file, the bound, and the recent attempts."""
        elapsed = self._clock() - self._cleared_at
        recent = ', '.join(f'{attempt.outcome} {attempt.result}' for attempt in self._recent)
        return CheckpointBoundError(
            f'checkpoint {bound} bound exceeded for {self._path}: {detail}; '
            f'{elapsed:.3f}s since the last complete or reset-observed attempt; '
            f'recent attempts: {recent}',
            self._recent[-1],
        )

    def _passive_checkpoint(self) -> CheckpointResult:
        """Run one passive checkpoint on the owned connection."""
        try:
            row = self._connection.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
        except sqlite3.Error as exc:
            raise DatabaseError(str(exc)) from exc
        if row is None:
            raise DatabaseError('SQLite returned no passive checkpoint result')
        busy, log_frames, checkpointed = (int(value) for value in row)
        return busy, log_frames, checkpointed

    def _record(self, attempt: CheckpointAttempt) -> None:
        """Update this checkpointer's history and raise when a bound is exceeded."""
        self._recent.append(attempt)
        cleared = attempt.outcome in _CLEARING_OUTCOMES
        self._state = 'complete' if cleared else 'incomplete'
        if cleared:
            self._cleared_at = attempt.finished_at
        _busy, log_frames, checkpointed = attempt.result
        if attempt.outcome != 'contended':
            self._previous_log = log_frames
            self._previous_checkpointed = checkpointed
            if log_frames * self._frame_bytes > self._wal_limit_bytes:
                raise self._halt(
                    'WAL growth',
                    f'{log_frames} committed frames exceed {self._wal_limit_bytes} bytes',
                )
        incomplete_secs = attempt.finished_at - self._cleared_at
        if incomplete_secs >= _CHECKPOINT_FAILURE_INTERVALS * self._interval_secs:
            raise self._halt(
                'incomplete duration',
                f'{_CHECKPOINT_FAILURE_INTERVALS} intervals of {self._interval_secs}s elapsed',
            )

    @property
    def frame_bytes(self) -> int:
        """Return the bytes one WAL frame occupies: the page size plus its header."""
        return self._frame_bytes

    def run_once(self) -> CheckpointAttempt:
        """Make one checkpoint attempt and enforce both bounds.

        Returns:
            The classified attempt.

        Raises:
            CheckpointBoundError: The classified attempt exceeded the
                incomplete-duration bound or the WAL growth tripwire.
            DatabaseError: SQLite failed before an attempt could be classified.
        """
        result = self._run_checkpoint()
        retried = result[0] != 0
        if retried:
            self._sleep(self._random_value() * self._interval_secs / 2)
            result = self._run_checkpoint()
        attempt = CheckpointAttempt(
            outcome=self._classify(result),
            result=result,
            retried=retried,
            finished_at=self._clock(),
        )
        self._record(attempt)
        return attempt

    @property
    def state(self) -> CheckpointState:
        """Return `idle` before the first attempt, then `complete` or `incomplete`."""
        return self._state


def _page_size(connection: sqlite3.Connection) -> int:
    """Return the database page size that sizes each WAL frame."""
    try:
        row = connection.execute('PRAGMA page_size').fetchone()
    except sqlite3.Error as exc:
        raise DatabaseError(str(exc)) from exc
    if row is None:
        raise DatabaseError('SQLite returned no page size')
    return int(row[0])
