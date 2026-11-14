"""Schedule process-local close-out retries on the synchronized source clock."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from collections.abc import Collection, Sequence

ObligationKey: TypeAlias = tuple[int, str]


class CloseoutRetries:
    """Keep one process's retry schedule for a group's pending obligations.

    All clocks are synchronized source successful-query seconds. Records may
    precede grace when an initial fetch fails before confirming an omission.
    """

    def __init__(self, *, fetch_interval_secs: float, window_secs: float) -> None:
        """Set the shared retry cadence and each gap's grace duration in seconds."""
        self._interval = fetch_interval_secs
        self._window = window_secs
        self._records: dict[ObligationKey, _Retry] = {}
        self._last_turn: float | None = None

    def failed(self, key: ObligationKey, clock: float) -> None:
        """Retry an incomplete pass after one interval without changing its grace."""
        record = self._records.get(key)
        if record is None:
            self._records[key] = _Retry(None, self._interval, clock + self._interval)
        else:
            record.due = clock + self._interval

    def grace_ended(self, key: ObligationKey, clock: float) -> bool:
        """Report whether a confirmed omission began at least one window ago."""
        record = self._records.get(key)
        return (
            record is not None
            and record.grace_start is not None
            and clock >= record.grace_start + self._window
        )

    def keep(self, keys: Collection[ObligationKey]) -> None:
        """Discard retry records outside the current generation's pending set."""
        self._records = {key: record for key, record in self._records.items() if key in keys}

    def missed(self, key: ObligationKey, clock: float) -> bool:
        """Schedule a confirmed omission and report whether its grace just began."""
        record = self._records.get(key)
        if record is None or record.grace_start is None:
            self._records[key] = _Retry(clock, self._interval, clock + self._interval)
            return True
        record.wait *= 2
        record.due = min(clock + record.wait, record.grace_start + self._window)
        return False

    def next_retry(self, keys: Sequence[ObligationKey], clock: float) -> ObligationKey | None:
        """Choose the longest-due eligible gap, breaking ties by input order.

        Args:
            keys: Serviceable gaps in ascending low-ID order.
            clock: Synchronized source successful-query seconds.

        Returns:
            One due key, or `None` when no key is due or the shared cadence
            does not yet permit a retry.
        """
        if self._last_turn is not None and clock - self._last_turn < self._interval:
            return None
        due = [key for key in keys if key in self._records and self._records[key].due <= clock]
        return min(due, key=lambda key: self._records[key].due) if due else None

    def retrying(self, key: ObligationKey) -> bool:
        """Report whether a gap has retry state, including before its first miss."""
        return key in self._records

    def take_turn(self, clock: float) -> None:
        """Record the synchronized source clock of the group's latest retry pass."""
        self._last_turn = clock


@dataclass
class _Retry:
    """Keep one gap's grace and next retry independent of provider failures."""

    grace_start: float | None
    wait: float
    due: float
