from __future__ import annotations

from typing import TYPE_CHECKING, NotRequired, TypedDict, Unpack

from sg_event_daemon._health import aggregate_status, evaluate_health
from sg_event_daemon._probe import ProbeResult

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from sg_event_daemon._health import (
        BreakerTransition,
        GroupHealth,
        WorkerHealth,
        _StatusSnapshot,
    )
    from sg_event_daemon._source import SourceHealth


class _HealthStatusOptions(TypedDict):
    sources: Iterable[SourceHealth]
    groups: Iterable[GroupHealth]
    workers: Iterable[WorkerHealth]
    now: float
    stale_after_secs: float
    audit_count: NotRequired[int]
    database_sizes: NotRequired[Iterable[tuple[str, int]]]
    breaker_history: NotRequired[Sequence[BreakerTransition]]
    breaker_transition_count: NotRequired[int]
    worker_breaker_open: NotRequired[bool]
    supervisor_conditions: NotRequired[Sequence[str]]
    inventory_complete: NotRequired[bool]


def status_from_health(**options: Unpack[_HealthStatusOptions]) -> _StatusSnapshot:
    evaluation = evaluate_health(
        sources=options['sources'],
        groups=options['groups'],
        workers=options['workers'],
        now=options['now'],
        stale_after_secs=options['stale_after_secs'],
        worker_breaker_open=options.get('worker_breaker_open', False),
        supervisor_conditions=options.get('supervisor_conditions', ()),
        inventory_complete=options.get('inventory_complete', True),
    )
    probe = ProbeResult(
        liveness=evaluation.liveness,
        readiness=evaluation.readiness,
        complete=options.get('inventory_complete', True),
        conditions=evaluation.conditions,
        generated_at=options['now'],
        snapshot_age_seconds=0.0,
    )
    return aggregate_status(
        sources=evaluation.sources,
        groups=evaluation.groups,
        workers=evaluation.workers,
        now=options['now'],
        probe=probe,
        audit_count=options.get('audit_count', 0),
        database_sizes=options.get('database_sizes', ()),
        breaker_history=options.get('breaker_history', ()),
        breaker_transition_count=options.get('breaker_transition_count', 0),
    )
