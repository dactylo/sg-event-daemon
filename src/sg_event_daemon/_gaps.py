"""Represent missing event IDs and log gap transitions."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

IdRun: TypeAlias = tuple[int, int]

_GROUP_LOGGER = logging.getLogger('sg_event_daemon.group')
_MAX_LOGGED_RUNS = 10
_SOURCE_LOGGER = logging.getLogger('sg_event_daemon.source')


def _count_ids(runs: Iterable[IdRun]) -> int:
    """Count the IDs covered by inclusive runs."""
    return sum(high - low + 1 for low, high in runs)


def _format_runs(runs: Sequence[IdRun]) -> str:
    """List up to ten runs, followed by the count of omitted runs."""
    shown = [str(low) if low == high else f'{low}-{high}' for low, high in runs[:_MAX_LOGGED_RUNS]]
    formatted = ', '.join(shown)
    if len(runs) > _MAX_LOGGED_RUNS:
        formatted += f' and {len(runs) - _MAX_LOGGED_RUNS} more'
    return formatted


def clip_runs(
    runs: Iterable[IdRun], *, low: int | None = None, high: int | None = None
) -> tuple[IdRun, ...]:
    """Keep the part of each run inside the inclusive bounds."""
    clipped: list[IdRun] = []
    for run_low, run_high in runs:
        clipped_low = max(run_low, low) if low is not None else run_low
        clipped_high = min(run_high, high) if high is not None else run_high
        if clipped_low <= clipped_high:
            clipped.append((clipped_low, clipped_high))
    return tuple(clipped)


def id_runs(event_ids: Iterable[int]) -> tuple[IdRun, ...]:
    """Group strictly ascending IDs into maximal inclusive runs.

    Raises:
        ValueError: IDs repeat or descend.
    """
    ids = iter(event_ids)
    try:
        low = high = next(ids)
    except StopIteration:
        return ()

    runs: list[IdRun] = []
    for event_id in ids:
        if event_id <= high:
            raise ValueError('event IDs must strictly ascend')
        if event_id == high + 1:
            high = event_id
            continue
        runs.append((low, high))
        low = high = event_id
    runs.append((low, high))
    return tuple(runs)


def log_catchup_skipped(extra: Mapping[str, str], low: int, high: int, secs: float) -> None:
    """Warn that catch-up skipped IDs absent on two reads."""
    _GROUP_LOGGER.warning(
        'no events in IDs %s-%s on two reads %.1fs apart; skipped them; '
        'replay the group from %s if they appear',
        low,
        high,
        secs,
        low,
        extra=dict(extra),
    )


def log_closeout_gave_up(
    extra: Mapping[str, str], low: int, high: int, runs: Sequence[IdRun]
) -> None:
    """Warn that a group skipped events missing on close-out.

    Args:
        extra: Structured site, source, and group log context.
        low: Inclusive low ID of the closed range.
        high: Inclusive high ID of the closed range.
        runs: Nonempty ascending inclusive runs of IDs still missing.
    """
    _GROUP_LOGGER.warning(
        'gave up on %s events SG did not return in IDs %s-%s: %s; '
        'replay the group from %s to run them if they become readable',
        _count_ids(runs),
        low,
        high,
        _format_runs(runs),
        runs[0][0],
        extra=dict(extra),
    )


def log_closeout_retrying(
    extra: Mapping[str, str], low: int, high: int, runs: Sequence[IdRun], secs: float
) -> None:
    """Warn that a group close-out read missed source-visible events."""
    _GROUP_LOGGER.warning(
        'SG did not return %s events the source saw in IDs %s-%s: %s; '
        'retrying, and skipping them if still missing after %.1fs',
        _count_ids(runs),
        low,
        high,
        _format_runs(runs),
        secs,
        extra=dict(extra),
    )


def log_gap_filled(extra: Mapping[str, str], low: int, high: int, secs: float) -> None:
    """Report that every ID in a source gap appeared."""
    _SOURCE_LOGGER.info(
        'late events in IDs %s-%s all appeared; gap closed after %.1fs',
        low,
        high,
        secs,
        extra=dict(extra),
    )


def log_ids_given_up(extra: Mapping[str, str], runs: Sequence[IdRun], secs: float) -> None:
    """Warn about source gap IDs that never appeared."""
    _SOURCE_LOGGER.warning(
        'gave up on %s event IDs after %.1fs: %s',
        _count_ids(runs),
        secs,
        _format_runs(runs),
        extra=dict(extra),
    )


def log_paused(extra: Mapping[str, str], setting: str, limit: int, count: int) -> None:
    """Warn that a source capacity limit has paused admission."""
    _SOURCE_LOGGER.warning(
        'source paused: %s limit %s reached (%s); open gaps continue and new gaps wait',
        setting,
        limit,
        count,
        extra=dict(extra),
    )


def log_resumed(extra: Mapping[str, str], secs: float) -> None:
    """Report how long a source was paused."""
    _SOURCE_LOGGER.info('source resumed after %.1fs paused', secs, extra=dict(extra))


def subtract_ids(runs: Iterable[IdRun], present: Iterable[int]) -> tuple[IdRun, ...]:
    """Remove ascending present IDs from ascending inclusive runs."""
    present_ids = iter(present)
    event_id = next(present_ids, None)
    missing: list[IdRun] = []
    for low, high in runs:
        cursor = low
        while event_id is not None and event_id < low:
            event_id = next(present_ids, None)
        while event_id is not None and event_id <= high:
            if event_id > cursor:
                missing.append((cursor, event_id - 1))
            cursor = max(cursor, event_id + 1)
            event_id = next(present_ids, None)
        if cursor <= high:
            missing.append((cursor, high))
    return tuple(missing)
