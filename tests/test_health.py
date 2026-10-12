from __future__ import annotations

import hashlib
import io
from dataclasses import asdict, fields, replace
from typing import TYPE_CHECKING, Literal, NotRequired, TypedDict, Unpack

import pytest

import sg_event_daemon._health as health_module
from sg_event_daemon._config import SecretValue
from sg_event_daemon._health import (
    _STATUS_PRECEDENCE,
    BreakerTransition,
    EventSocketHealth,
    GroupHealth,
    WatchdogHealth,
    WorkerHealth,
    aggregate_status,
    bound_health_text,
)
from sg_event_daemon._lane import LaneHealth
from sg_event_daemon._logging import configure_logging, register_redaction_values
from sg_event_daemon._source import SOURCE_RUN_STATUSES, SourceHealth

if TYPE_CHECKING:
    from sg_event_daemon._watchdog import WatchdogKind, WatchdogPhase


class _GroupOverrides(TypedDict):
    conditions: NotRequired[tuple[str, ...]]
    failure_kind: NotRequired[str | None]
    failure_text: NotRequired[str | None]
    join_phase: NotRequired[str]
    parked: NotRequired[bool]
    park_reason: NotRequired[str | None]
    pending_catchup_epoch_count: NotRequired[int]


def _lane() -> LaneHealth:
    return LaneHealth(
        site='main',
        source='events',
        group='editorial',
        worker_instance='spawn-1',
        execution='sync',
        ordering='strict',
        phase='idle',
        status='running',
        cursor=99,
        event_id=None,
        plugin_id=None,
        callback_id=None,
        delivery_attempt=None,
        heartbeat_at=95.0,
        heartbeat_age=0.0,
        deadline_at=105.0,
        internal_stall_deadline_secs=10.0,
        overdue_reason=None,
    )


def _group(**overrides: Unpack[_GroupOverrides]) -> GroupHealth:
    return GroupHealth.from_lane(
        _lane(),
        queue_event_count=2,
        queue_byte_count=200,
        catchup_frontier=100,
        pending_catchup_epoch_count=overrides.get('pending_catchup_epoch_count', 2),
        oldest_catchup_epoch_age=4.0,
        join_phase=overrides.get('join_phase', 'attached'),
        outstanding_late_obligation_count=1,
        outstanding_late_id_count=2,
        outstanding_late_ids=(event_id for event_id in (12, 13)),
        outstanding_late_ids_truncated=False,
        parked=overrides.get('parked', False),
        park_reason=overrides.get('park_reason'),
        failure_kind=overrides.get('failure_kind'),
        failure_text=overrides.get('failure_text'),
        checkpoint_state='complete',
        conditions=(condition for condition in overrides.get('conditions', ())),
    )


def _source(
    *,
    retry_attempt: int = 0,
    next_retry_secs: float | None = None,
) -> SourceHealth:
    return SourceHealth(
        site='main',
        source='events',
        source_generation='source-generation',
        status='running',
        frontier=100,
        successful_clock=80.0,
        retry_attempt=retry_attempt,
        next_retry_secs=next_retry_secs,
        open_range_count=1,
        late_id_count=2,
        last_error=None,
        phase='wait',
        heartbeat_at=95.0,
        heartbeat_age=0.0,
        deadline_at=105.0,
        overdue_reason=None,
        worker_instance='spawn-1',
        initialized=True,
        pending_obligation_count=3,
        orphan_marker_count=4,
        strict_buffer_event_count=7,
        strict_buffer_byte_count=8,
        strict_retention=(('editorial', 90, 7, 8),),
        checkpoint_state='complete',
    )


def _watchdog(
    kind: WatchdogKind,
    phase: WatchdogPhase,
    *,
    deadline_at: float | None,
) -> WatchdogHealth:
    return WatchdogHealth(
        kind=kind,
        site='main',
        source=None if kind == 'operator' and phase == 'idle' else 'events',
        group='editorial' if kind == 'lane' else None,
        worker_instance='spawn-1',
        phase=phase,
        heartbeat_at=95.0,
        deadline_at=deadline_at,
        event_id=99 if phase == 'callback' else None,
        plugin_id='plugin-a' if phase == 'callback' else None,
        callback_id='callback-a' if phase == 'callback' else None,
        delivery_attempt=2 if phase == 'callback' else None,
        operator_command='park' if kind == 'operator' and phase != 'idle' else None,
    )


def _worker(
    *,
    breaker_open: bool = False,
    heartbeat_at: float = 99.0,
    watchdog_failure: str | None = None,
) -> WorkerHealth:
    return WorkerHealth(
        site='main',
        worker_id='["site","main","main"]',
        spawn_instance='spawn-1',
        heartbeat_at=heartbeat_at,
        monitor_alive=True,
        thread_names=('source:events', 'lane:editorial'),
        stuck_threads=(),
        breaker_open=breaker_open,
        breaker_reason=None,
        socket_buffer_requested=8192,
        socket_send_buffer=16_384,
        socket_receive_buffer=16_384,
        watchdog_failure=watchdog_failure,
    )


def test_aggregate_status_uses_exact_primary_precedence() -> None:
    assert _STATUS_PRECEDENCE == (
        'worker-breaker-open',
        'stale',
        'stream-identity-changed',
        'ordering-changed',
        'event-unavailable',
        'history-truncated',
        'ordering-anomaly',
        'capacity-exhausted',
        'callback-failed',
        'source-degraded',
        'catching-up',
        'parked',
        'idle',
        'healthy',
    )
    group = _group(
        conditions=(
            'callback-failed',
            'catching-up',
            'ordering-anomaly',
        )
    )
    sources = (_source(retry_attempt=1, next_retry_secs=2.0),)
    groups = (group,)
    workers = (_worker(breaker_open=True),)
    status = aggregate_status(
        sources=(source for source in sources),
        groups=(item for item in groups),
        workers=(worker for worker in workers),
        now=100.0,
        stale_after_secs=10.0,
        database_sizes=(item for item in (('source.db', 10),)),
    )

    assert status.primary_state == 'worker-breaker-open'
    assert set(status.conditions) >= {
        'worker-breaker-open',
        'source-degraded',
        'ordering-anomaly',
        'callback-failed',
        'catching-up',
    }
    assert status.database_sizes == (('source.db', 10),)
    assert 'worker-breaker-open' in health_module.probe_health_conditions(
        sources=(source for source in sources),
        groups=(item for item in groups),
        workers=(worker for worker in workers),
        now=100.0,
        stale_after_secs=10.0,
        worker_breaker_open=True,
    )
    assert not health_module.probe_health_ready(
        sources=(source for source in sources),
        groups=(item for item in groups),
        workers=(worker for worker in workers),
        now=100.0,
        stale_after_secs=10.0,
        worker_breaker_open=True,
    )
    assert (
        health_module.primary_state_for_conditions(dict.fromkeys(('idle', 'healthy')).keys())
        == 'idle'
    )


def test_authoritative_lane_capacity_names_always_produce_status() -> None:
    lane = replace(_lane(), phase='callback')
    for condition in ('late-capacity-exhausted', 'strict-capacity-exhausted'):
        group = GroupHealth.from_lane(
            lane,
            queue_event_count=0,
            queue_byte_count=0,
            catchup_frontier=None,
            pending_catchup_epoch_count=0,
            oldest_catchup_epoch_age=None,
            join_phase='attached',
            outstanding_late_obligation_count=0,
            outstanding_late_id_count=0,
            outstanding_late_ids=(),
            outstanding_late_ids_truncated=False,
            parked=False,
            park_reason=None,
            failure_kind=None,
            failure_text=None,
            checkpoint_state='complete',
            conditions=(condition,),
        )

        status = aggregate_status(
            sources=(_source(),),
            groups=(group,),
            workers=(_worker(),),
            now=100.0,
            stale_after_secs=10.0,
        )

        assert status.primary_state == 'capacity-exhausted'
        assert status.conditions == ('capacity-exhausted',)


def test_empty_poll_is_ready_while_open_breaker_stays_live() -> None:
    empty_poll = aggregate_status(
        sources=(_source(),),
        groups=(),
        workers=(_worker(),),
        now=100.0,
        stale_after_secs=10.0,
    )
    open_breaker = aggregate_status(
        sources=(_source(),),
        groups=(),
        workers=(_worker(breaker_open=True),),
        now=100.0,
        stale_after_secs=10.0,
    )

    assert empty_poll.liveness
    assert empty_poll.readiness
    assert open_breaker.liveness
    assert not open_breaker.readiness
    assert open_breaker.primary_state == 'worker-breaker-open'


def test_every_source_halt_status_maps_to_a_condition() -> None:
    halt_statuses = set(SOURCE_RUN_STATUSES) - {'running', 'uninitialized'}
    assert set(health_module._SOURCE_HALT_CONDITIONS) == halt_statuses
    assert set(health_module._SOURCE_HALT_CONDITIONS.values()) == {
        'capacity-exhausted',
        'event-unavailable',
        'stream-identity-changed',
    }


def test_group_health_retains_bounded_late_id_prefix_and_exact_late_id_count() -> None:
    late_ids = tuple(range(100))
    truncated = replace(
        _group(),
        outstanding_late_id_count=50_000,
        outstanding_late_ids=late_ids,
        outstanding_late_ids_truncated=True,
    )
    complete = replace(
        truncated,
        outstanding_late_id_count=100,
        outstanding_late_ids_truncated=False,
    )

    assert truncated.outstanding_late_id_count == 50_000
    assert truncated.outstanding_late_ids == late_ids
    assert truncated.outstanding_late_ids_truncated is True
    assert complete.outstanding_late_ids_truncated is False

    with pytest.raises(ValueError, match='late-id count'):
        replace(complete, outstanding_late_id_count=99)
    with pytest.raises(ValueError, match='late-id count'):
        replace(
            complete,
            outstanding_late_id_count=-1,
            outstanding_late_ids=(),
        )
    with pytest.raises(ValueError, match='truncation'):
        replace(truncated, outstanding_late_ids_truncated=False)
    with pytest.raises(ValueError, match='truncation'):
        replace(complete, outstanding_late_ids_truncated=True)


def test_owner_health_retains_exact_audit_count_and_database_size() -> None:
    source = replace(_source(), audit_count=11, database_size_bytes=12_345)
    group = replace(_group(), audit_count=13)

    assert (source.audit_count, source.database_size_bytes) == (11, 12_345)
    assert group.audit_count == 13
    assert 'database_size_bytes' not in asdict(group)


def test_park_affects_neither_probe_and_callback_only_readiness() -> None:
    parked = aggregate_status(
        sources=(_source(),),
        groups=(
            _group(
                parked=True,
                park_reason='operator',
                pending_catchup_epoch_count=0,
                join_phase='parked',
                conditions=('parked',),
            ),
        ),
        workers=(_worker(),),
        now=100.0,
        stale_after_secs=10.0,
    )
    failed = aggregate_status(
        sources=(_source(),),
        groups=(
            _group(
                failure_kind='callback',
                failure_text='token=abc',
                pending_catchup_epoch_count=0,
                join_phase='attached',
                conditions=('callback-failed',),
            ),
        ),
        workers=(_worker(),),
        now=100.0,
        stale_after_secs=10.0,
    )

    assert parked.liveness
    assert parked.readiness
    assert parked.primary_state == 'parked'
    assert failed.liveness
    assert not failed.readiness
    assert failed.groups[0].failure_text == '<redacted>'


@pytest.mark.parametrize(
    ('blocker_owner', 'expected_condition'),
    [
        pytest.param('source', 'source-degraded', id='source'),
        pytest.param('worker', 'worker-breaker-open', id='worker'),
        pytest.param('supervisor', 'worker-breaker-open', id='supervisor-extra'),
    ],
)
def test_parked_group_failure_does_not_hide_non_group_blocker(
    blocker_owner: Literal['source', 'worker', 'supervisor'],
    expected_condition: str,
) -> None:
    parked_failure = _group(
        parked=True,
        failure_kind='callback',
        failure_text='failed',
        join_phase='parked',
        pending_catchup_epoch_count=0,
        conditions=('parked', 'callback-failed'),
    )

    status = aggregate_status(
        sources=(
            (
                _source(retry_attempt=1, next_retry_secs=2.0)
                if blocker_owner == 'source'
                else _source()
            ),
        ),
        groups=(parked_failure,),
        workers=(_worker(breaker_open=blocker_owner == 'worker'),),
        now=100.0,
        stale_after_secs=10.0,
        worker_breaker_open=blocker_owner == 'supervisor',
    )

    assert not status.readiness
    assert expected_condition in status.conditions
    assert 'callback-failed' in status.conditions


def test_parked_group_failure_is_visible_without_blocking_readiness() -> None:
    parked_failure = _group(
        parked=True,
        failure_kind='callback',
        failure_text='failed',
        join_phase='parked',
        pending_catchup_epoch_count=0,
        conditions=('parked', 'callback-failed'),
    )

    status = aggregate_status(
        sources=(_source(),),
        groups=(parked_failure,),
        workers=(_worker(),),
        now=100.0,
        stale_after_secs=10.0,
    )

    assert status.readiness
    assert status.primary_state == 'callback-failed'
    assert 'parked' in status.conditions
    assert 'callback-failed' in status.conditions


def test_public_phase_health_validates_time_and_bounds_reasons() -> None:
    diagnostic = 'x' * 5_000

    source = replace(_source(), overdue_reason=diagnostic)
    group = replace(_group(), overdue_reason=diagnostic)
    worker = _worker(watchdog_failure=diagnostic)

    assert source.overdue_reason == 'x' * 4_096
    assert group.overdue_reason == 'x' * 4_096
    assert worker.watchdog_failure == 'x' * 4_096
    with pytest.raises(ValueError, match='finite and nonnegative'):
        replace(source, heartbeat_age=float('inf'))
    with pytest.raises(ValueError, match='finite and nonnegative'):
        replace(group, deadline_at=-1.0)


def test_registered_health_text_is_redacted_before_utf8_bounding() -> None:
    protected_value = 'registered-health-value-' + 'x' * 4096
    protected_digest = hashlib.sha256(protected_value.encode()).hexdigest()
    configure_logging(stream=io.StringIO())
    register_redaction_values(
        SecretValue(protected_value),
        SecretValue(protected_digest),
    )

    result = bound_health_text(f'before {protected_value} digest={protected_digest} after')

    assert result == 'before <redacted> digest=<redacted> after'
    assert protected_value not in result
    assert protected_digest not in result
    assert '<redacted>' in result


def test_status_preserves_depth_join_capacity_and_audit_evidence() -> None:
    transitions = tuple(
        BreakerTransition(
            sequence=index,
            worker_id='worker',
            state='open' if index % 2 else 'closed',
            reason=f'transition-{index}',
        )
        for index in range(105)
    )
    status = aggregate_status(
        sources=(_source(),),
        groups=(
            _group(
                conditions=(
                    'event-unavailable',
                    'history-truncated',
                    'stream-identity-changed',
                    'ordering-changed',
                    'capacity-exhausted',
                )
            ),
        ),
        workers=(_worker(),),
        now=100.0,
        stale_after_secs=10.0,
        audit_count=123_456,
        database_sizes=(('supervisor.db', 4096), ('source.db', 8192)),
        breaker_history=transitions,
        breaker_transition_count=900,
    )
    payload = status.to_mapping()

    assert status.groups[0].queue_event_count == 2
    assert status.groups[0].queue_byte_count == 200
    assert status.groups[0].pending_catchup_epoch_count == 2
    assert status.groups[0].oldest_catchup_epoch_age == 4.0
    assert status.groups[0].outstanding_late_ids == (12, 13)
    assert status.audit_count == 123_456
    assert status.database_sizes == (('supervisor.db', 4096), ('source.db', 8192))
    assert len(status.breaker_history) == 100
    assert status.breaker_history[0].sequence == 5
    assert status.breaker_transition_count == 900
    assert payload['primary_state'] == 'stream-identity-changed'


def test_structured_health_contains_status_evidence_fields() -> None:
    source_names = {field.name for field in fields(SourceHealth)}
    assert {
        'site',
        'source',
        'worker_instance',
        'initialized',
        'frontier',
        'successful_clock',
        'retry_attempt',
        'next_retry_secs',
        'open_range_count',
        'pending_obligation_count',
        'late_id_count',
        'orphan_marker_count',
        'strict_buffer_event_count',
        'strict_buffer_byte_count',
        'strict_retention',
        'checkpoint_state',
        'stream_incarnation',
        'audit_count',
        'database_size_bytes',
        'phase',
        'heartbeat_at',
        'heartbeat_age',
        'deadline_at',
        'overdue_reason',
    } <= source_names
    assert tuple(field.name for field in fields(SourceHealth))[-7:] == (
        'audit_count',
        'database_size_bytes',
        'phase',
        'heartbeat_at',
        'heartbeat_age',
        'deadline_at',
        'overdue_reason',
    )
    group_names = {field.name for field in fields(GroupHealth)}
    assert {
        'site',
        'source',
        'group',
        'worker_instance',
        'execution',
        'ordering',
        'cursor',
        'current_event_id',
        'current_plugin_id',
        'current_callback_id',
        'delivery_attempt',
        'heartbeat_age',
        'queue_event_count',
        'queue_byte_count',
        'catchup_frontier',
        'pending_catchup_epoch_count',
        'oldest_catchup_epoch_age',
        'join_phase',
        'outstanding_late_obligation_count',
        'outstanding_late_id_count',
        'outstanding_late_ids',
        'outstanding_late_ids_truncated',
        'parked',
        'park_reason',
        'failure_kind',
        'failure_text',
        'checkpoint_state',
        'audit_count',
        'deadline_at',
        'overdue_reason',
    } <= group_names
    assert tuple(field.name for field in fields(GroupHealth))[-3:] == (
        'audit_count',
        'deadline_at',
        'overdue_reason',
    )


def test_supervisor_clock_computes_stale_and_idle() -> None:
    status = aggregate_status(
        sources=(_source(),),
        groups=(_group(),),
        workers=(_worker(heartbeat_at=80.0),),
        now=100.0,
        stale_after_secs=10.0,
    )

    assert status.workers[0].heartbeat_age == 20.0
    assert status.primary_state == 'stale'
    assert not status.liveness
    assert not status.readiness


def test_supervisor_clock_recomputes_source_age_and_overdue_readiness() -> None:
    status = aggregate_status(
        sources=(replace(_source(), heartbeat_at=90.0, overdue_reason='source-phase-overdue'),),
        groups=(),
        workers=(_worker(),),
        now=100.0,
        stale_after_secs=10.0,
    )

    assert status.sources[0].heartbeat_age == 10.0
    assert status.primary_state == 'stale'
    assert not status.readiness


def test_unparked_group_failure_blocks_beside_parked_group_failure() -> None:
    parked_failure = replace(
        _group(
            parked=True,
            failure_kind='callback',
            failure_text='parked failure',
            join_phase='parked',
            pending_catchup_epoch_count=0,
            conditions=('parked', 'callback-failed'),
        ),
        group='parked',
    )
    active_failure = replace(
        _group(
            parked=False,
            failure_kind='setup',
            failure_text='active failure',
            join_phase='attached',
            pending_catchup_epoch_count=0,
            conditions=('callback-failed',),
        ),
        group='active',
    )

    status = aggregate_status(
        sources=(_source(),),
        groups=(parked_failure, active_failure),
        workers=(_worker(),),
        now=100.0,
        stale_after_secs=10.0,
    )

    assert not status.readiness
    assert status.primary_state == 'callback-failed'
    assert 'parked' in status.conditions


def test_unparked_group_failure_still_blocks_readiness() -> None:
    failed = _group(
        parked=False,
        failure_kind='callback',
        failure_text='failed',
        join_phase='attached',
        pending_catchup_epoch_count=0,
        conditions=('callback-failed',),
    )

    status = aggregate_status(
        sources=(_source(),),
        groups=(failed,),
        workers=(_worker(),),
        now=100.0,
        stale_after_secs=10.0,
    )

    assert not status.readiness
    assert status.primary_state == 'callback-failed'


def test_watchdog_failure_uses_existing_stale_primary_state() -> None:
    status = aggregate_status(
        sources=(),
        groups=(),
        workers=(_worker(watchdog_failure='watchdog-sampling-failed'),),
        now=100.0,
        stale_after_secs=10.0,
    )

    assert status.primary_state == 'stale'
    assert status.conditions == ('stale',)
    assert not status.readiness


@pytest.mark.parametrize(
    ('kind', 'phases'),
    [
        (
            'source',
            (
                'startup',
                'provider-query',
                'internal',
                'wait',
                'shutdown',
                'write-lock-wait',
                'write-hold',
            ),
        ),
        (
            'lane',
            (
                'startup',
                'setup',
                'callback',
                'internal',
                'idle',
                'shutdown',
                'write-lock-wait',
                'write-hold',
            ),
        ),
        ('operator', ('idle', 'internal', 'shutdown')),
    ],
)
def test_watchdog_health_accepts_only_kind_specific_phases(
    kind: WatchdogKind,
    phases: tuple[WatchdogPhase, ...],
) -> None:
    all_phases: tuple[WatchdogPhase, ...] = (
        'callback',
        'idle',
        'internal',
        'provider-query',
        'setup',
        'shutdown',
        'startup',
        'wait',
        'write-hold',
        'write-lock-wait',
    )
    for phase in all_phases:
        deadline_at = None if kind == 'operator' and phase == 'idle' else 105.0
        if phase in phases:
            assert _watchdog(kind, phase, deadline_at=deadline_at).phase == phase
        else:
            with pytest.raises(ValueError, match='phase'):
                _watchdog(kind, phase, deadline_at=deadline_at)


@pytest.mark.parametrize(
    ('kind', 'phase', 'deadline_at'),
    [
        ('source', 'wait', None),
        ('lane', 'idle', None),
        ('operator', 'internal', None),
        ('operator', 'idle', 105.0),
    ],
)
def test_watchdog_health_enforces_phase_deadline_shape(
    kind: WatchdogKind,
    phase: WatchdogPhase,
    deadline_at: float | None,
) -> None:
    with pytest.raises(ValueError, match='deadline'):
        _watchdog(kind, phase, deadline_at=deadline_at)


def test_watchdog_health_excludes_worker_local_fence_fields() -> None:
    payload = asdict(_watchdog('lane', 'callback', deadline_at=105.0))

    assert tuple(payload) == (
        'kind',
        'site',
        'source',
        'group',
        'worker_instance',
        'phase',
        'heartbeat_at',
        'deadline_at',
        'event_id',
        'plugin_id',
        'callback_id',
        'delivery_attempt',
        'operator_command',
    )


@pytest.mark.parametrize('value', [-1.0, float('inf'), float('nan')])
@pytest.mark.parametrize('field_name', ['deadline_at', 'heartbeat_at'])
def test_watchdog_health_rejects_invalid_times(field_name: str, value: float) -> None:
    watchdog = _watchdog('source', 'internal', deadline_at=105.0)

    with pytest.raises(ValueError, match='finite and nonnegative'):
        replace(watchdog, **{field_name: value})


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        ('x' * 4097, 'x' * 4096),
        ('é' * 2049, 'é' * 2048),
        ('x' * 4096 + ' token=abc', '<redacted>'),
    ],
)
def test_health_text_fields_are_utf8_bounded(value: str, expected: str) -> None:
    identity = 'identity-' + 'i' * 4097
    group = replace(
        _group(park_reason=value, failure_text=value),
        site=identity,
        source=identity,
        group=identity,
        worker_instance=identity,
    )
    socket_health = EventSocketHealth(
        source=identity,
        group=identity,
        role='client',
        connected=False,
        requested=1,
        send=2,
        receive=3,
        queue_event_count=4,
        queue_byte_count=5,
        reason=value,
    )
    worker = replace(
        _worker(),
        site=identity,
        worker_id=identity,
        spawn_instance=identity,
        breaker_reason=value,
        source_outages=(('events-a', value), ('events-b', value)),
    )
    transition = BreakerTransition(
        sequence=1,
        worker_id=identity,
        state='open',
        reason=value,
    )

    assert group.park_reason == expected
    assert group.failure_text == expected
    assert socket_health.reason == expected
    assert worker.breaker_reason == expected
    assert worker.source_outages == (('events-a', expected), ('events-b', expected))
    assert transition.reason == expected
    assert len(expected.encode()) <= 4096
    assert group.site == group.source == group.group == group.worker_instance == identity
    assert socket_health.source == socket_health.group == identity
    assert worker.site == worker.worker_id == worker.spawn_instance == identity
    assert transition.worker_id == identity
    assert group.execution == 'sync'
    assert socket_health.role == 'client'
    assert transition.state == 'open'


@pytest.mark.parametrize('phase', ['callback', 'idle', 'internal', 'setup'])
@pytest.mark.parametrize(
    'condition',
    ['orphaned', 'replay-required', 'shape-mismatch'],
)
def test_lifecycle_only_conditions_always_produce_status(
    phase: Literal['callback', 'idle', 'internal', 'setup'],
    condition: str,
) -> None:
    group = GroupHealth.from_lane(
        replace(_lane(), phase=phase),
        queue_event_count=0,
        queue_byte_count=0,
        catchup_frontier=None,
        pending_catchup_epoch_count=0,
        oldest_catchup_epoch_age=None,
        join_phase='attached',
        outstanding_late_obligation_count=0,
        outstanding_late_id_count=0,
        outstanding_late_ids=(),
        outstanding_late_ids_truncated=False,
        parked=False,
        park_reason=None,
        failure_kind=None,
        failure_text=None,
        checkpoint_state='complete',
        conditions=(condition,),
    )

    status = aggregate_status(
        sources=(_source(),),
        groups=(group,),
        workers=(_worker(),),
        now=100.0,
        stale_after_secs=10.0,
    )

    assert status.primary_state == ('idle' if phase == 'idle' else 'healthy')
    assert condition in status.conditions
    assert status.groups[0].conditions == (condition,)
    assert status.liveness
    assert not status.readiness


@pytest.mark.parametrize('join_phase', ['catching-up', 'detached', 'joining'])
def test_unattached_group_is_not_ready(join_phase: str) -> None:
    status = aggregate_status(
        sources=(_source(),),
        groups=(
            _group(
                pending_catchup_epoch_count=0,
                join_phase=join_phase,
            ),
        ),
        workers=(_worker(),),
        now=100.0,
        stale_after_secs=10.0,
    )

    assert status.primary_state == 'catching-up'
    assert status.liveness
    assert not status.readiness
