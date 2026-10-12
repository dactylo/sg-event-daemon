from __future__ import annotations

import ast
import threading
from dataclasses import asdict, dataclass, replace
from itertools import pairwise
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from sg_event_daemon._config import CredentialConfig, SecretValue
from sg_event_daemon._watchdog import (
    ComponentWatchdog,
    WatchdogKind,
    WatchdogPhase,
    internal_stall_deadline_secs,
    provider_budget_secs,
)

if TYPE_CHECKING:
    from collections.abc import Callable

_CELL_IDENTITY = 'registration-1'


@dataclass
class _Clock:
    _now: float = 10.0

    def __call__(self) -> float:
        return self._now


def _watchdog(clock: _Clock) -> ComponentWatchdog:
    return ComponentWatchdog(
        kind='lane',
        identity='serial',
        worker_instance='spawn-1',
        registration_token=_CELL_IDENTITY,
        startup_deadline_secs=30.0,
        clock=clock,
    )


def test_callback_heartbeat_slides_only_its_current_deadline() -> None:
    clock = _Clock()
    watchdog = _watchdog(clock)

    with watchdog.watch('callback', deadline_at=20.0):
        clock._now = 17.0
        watchdog.heartbeat(sliding_deadline_secs=9.0)
        sample = watchdog.sample()

    assert sample.heartbeat_at == 17.0
    assert sample.deadline_at == 26.0


def test_idle_restoration_refreshes_heartbeat_and_preserves_budget() -> None:
    clock = _Clock(_now=0.0)
    watchdog = _watchdog(clock)
    watchdog.transition('idle', deadline_at=11.0)
    idle = watchdog.sample()

    with watchdog.watch('internal', deadline_at=15.0):
        clock._now = 20.0
    restored = watchdog.sample()

    assert restored.registration_token == _CELL_IDENTITY
    assert restored.kind == 'lane'
    assert restored.identity == 'serial'
    assert restored.worker_instance == 'spawn-1'
    assert restored.phase == 'idle'
    assert restored.heartbeat_at == 20.0
    assert restored.deadline_at == 31.0
    assert restored.event_id is None
    assert restored.plugin_id is None
    assert restored.callback_id is None
    assert restored.delivery_attempt is None
    assert restored.operator_command is None
    assert restored.transition_sequence > idle.transition_sequence


def test_internal_stall_deadline_uses_one_validated_shared_formula() -> None:
    credential = CredentialConfig(
        name='test',
        script_name='test',
        api_key=SecretValue('secret'),
        timeout_secs=3.0,
        max_rpc_attempts=4,
        rpc_attempt_interval_secs=1.0,
    )
    assert provider_budget_secs(credential) == 15.0
    assert internal_stall_deadline_secs(2.0, credential) == 30.0
    assert internal_stall_deadline_secs(4.0, credential) == 40.0
    invalid = (
        (0.0, credential),
        (2.0, replace(credential, timeout_secs=0.0)),
        (2.0, replace(credential, max_rpc_attempts=0)),
        (2.0, replace(credential, rpc_attempt_interval_secs=-1.0)),
    )
    for arguments in invalid:
        with pytest.raises(ValueError, match='must be'):
            internal_stall_deadline_secs(*arguments)


def test_nested_watch_restores_outer_identity_latest_heartbeat_and_deadline() -> None:
    clock = _Clock()
    watchdog = _watchdog(clock)

    with watchdog.watch(
        'callback',
        deadline_at=25.0,
        event_id=41,
        plugin_id='plugin',
        callback_id='callback',
        delivery_attempt=2,
    ):
        clock._now = 12.0
        watchdog.heartbeat()
        with watchdog.watch('internal', deadline_at=18.0):
            clock._now = 16.0
            watchdog.heartbeat()
        restored = watchdog.sample()

    assert restored.phase == 'callback'
    assert restored.heartbeat_at == 16.0
    assert restored.deadline_at == 25.0
    assert restored.event_id == 41
    assert restored.plugin_id == 'plugin'
    assert restored.callback_id == 'callback'
    assert restored.delivery_attempt == 2
    startup = watchdog.sample()
    assert startup.phase == 'startup'
    assert startup.heartbeat_at == 16.0
    assert startup.deadline_at == 40.0


def test_non_idle_restoration_preserves_outer_absolute_deadline() -> None:
    clock = _Clock(_now=0.0)
    watchdog = _watchdog(clock)
    watchdog.transition(
        'callback',
        deadline_at=11.0,
        event_id=41,
        plugin_id='plugin',
        callback_id='callback',
        delivery_attempt=2,
    )

    with watchdog.watch('internal', deadline_at=15.0):
        clock._now = 20.0
    restored = watchdog.sample()

    assert restored.phase == 'callback'
    assert restored.heartbeat_at == 20.0
    assert restored.deadline_at == 11.0
    assert restored.event_id == 41
    assert restored.plugin_id == 'plugin'
    assert restored.callback_id == 'callback'
    assert restored.delivery_attempt == 2


@pytest.mark.parametrize(
    ('kind', 'phase', 'deadline'),
    [('source', 'startup', 40.0), ('lane', 'startup', 40.0), ('operator', 'idle', None)],
)
def test_watchdog_construction_uses_valid_initial_phase_per_kind(
    kind: WatchdogKind, phase: WatchdogPhase, deadline: float | None
) -> None:
    watchdog = ComponentWatchdog(
        kind=kind,
        identity='component',
        worker_instance='spawn-1',
        registration_token=_CELL_IDENTITY,
        startup_deadline_secs=30.0,
        clock=_Clock(),
    )
    sample = watchdog.sample()
    assert sample.phase == phase
    assert sample.deadline_at == deadline
    assert sample.heartbeat_at == 10.0


def test_watchdog_module_has_no_project_imports() -> None:
    path = Path(__file__).parents[1] / 'src' / 'sg_event_daemon' / '_watchdog.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))

    assert not any(
        (isinstance(node, ast.ImportFrom) and node.level)
        or (
            isinstance(node, ast.Import | ast.ImportFrom)
            and any(alias.name.startswith('sg_event_daemon') for alias in node.names)
        )
        for node in ast.walk(tree)
    )


@pytest.mark.parametrize(
    ('kind', 'phase'),
    [
        ('source', 'callback'),
        ('source', 'idle'),
        ('source', 'setup'),
        ('lane', 'provider-query'),
        ('lane', 'wait'),
        ('operator', 'callback'),
        ('operator', 'provider-query'),
        ('operator', 'setup'),
        ('operator', 'startup'),
        ('operator', 'wait'),
        ('operator', 'write-hold'),
        ('operator', 'write-lock-wait'),
    ],
)
def test_watchdog_rejects_phase_outside_kind(kind: WatchdogKind, phase: WatchdogPhase) -> None:
    watchdog = ComponentWatchdog(
        kind=kind,
        identity='component',
        worker_instance='spawn-1',
        registration_token=_CELL_IDENTITY,
        startup_deadline_secs=30.0,
        clock=_Clock(),
    )
    before = watchdog.sample()
    with pytest.raises(ValueError, match='phase'):
        watchdog.transition(phase, deadline_at=20.0)
    assert watchdog.sample() == before
    with pytest.raises(ValueError, match='phase'), watchdog.watch(phase, deadline_at=20.0):
        pytest.fail('invalid phase entered')
    assert watchdog.sample() == before


def test_watchdog_sampling_does_not_wait_for_watched_operation() -> None:
    clock = _Clock()
    watchdog = _watchdog(clock)
    entered = threading.Event()
    release = threading.Event()

    def block_in_callback() -> None:
        with watchdog.watch(
            'callback',
            deadline_at=25.0,
            event_id=41,
            plugin_id='plugin',
            callback_id='callback',
            delivery_attempt=2,
        ):
            entered.set()
            assert release.wait(1.0)

    owner = threading.Thread(target=block_in_callback)
    owner.start()
    try:
        assert entered.wait(1.0)
        sample = watchdog.sample()
        assert sample.phase == 'callback'
        assert sample.event_id == 41
        assert sample.plugin_id == 'plugin'
        assert sample.callback_id == 'callback'
        assert sample.delivery_attempt == 2
        assert owner.is_alive()
    finally:
        release.set()
        owner.join(1.0)
    assert not owner.is_alive()


def test_watchdog_sequences_increase_on_transition_enter_heartbeat_and_restore() -> None:
    clock = _Clock()
    watchdog = _watchdog(clock)
    sequences = [watchdog.sample().transition_sequence]

    watchdog.transition('idle', deadline_at=None)
    sequences.append(watchdog.sample().transition_sequence)
    with watchdog.watch('callback', deadline_at=25.0):
        sequences.append(watchdog.sample().transition_sequence)
        clock._now = 12.0
        watchdog.heartbeat(sliding_deadline_secs=15.0)
        sequences.append(watchdog.sample().transition_sequence)
    sequences.append(watchdog.sample().transition_sequence)

    assert all(later > earlier for earlier, later in pairwise(sequences))


def test_watchdog_starts_in_bounded_startup_with_primitive_sample() -> None:
    clock = _Clock()

    sample = _watchdog(clock).sample()

    assert asdict(sample) == {
        'registration_token': 'registration-1',
        'transition_sequence': 1,
        'kind': 'lane',
        'identity': 'serial',
        'worker_instance': 'spawn-1',
        'phase': 'startup',
        'heartbeat_at': 10.0,
        'deadline_at': 40.0,
        'event_id': None,
        'plugin_id': None,
        'callback_id': None,
        'delivery_attempt': None,
        'operator_command': None,
    }
    assert all(
        value is None or type(value) in {float, int, str} for value in asdict(sample).values()
    )


@pytest.mark.parametrize('kind', ['lane', 'source'])
@pytest.mark.parametrize('phase', ['write-lock-wait', 'write-hold'])
def test_write_phases_are_valid_for_lane_and_source_cells(
    kind: WatchdogKind, phase: WatchdogPhase
) -> None:
    clock = _Clock()
    watchdog = ComponentWatchdog(
        kind=kind,
        identity='component',
        worker_instance='spawn-1',
        registration_token=_CELL_IDENTITY,
        startup_deadline_secs=30.0,
        clock=clock,
    )

    with watchdog.watch(phase, deadline_at=20.0):
        assert watchdog.sample().phase == phase
        assert watchdog.sample().deadline_at == 20.0

    operator = ComponentWatchdog(
        kind='operator',
        identity='operator',
        worker_instance='spawn-1',
        registration_token=_CELL_IDENTITY,
        startup_deadline_secs=30.0,
        clock=clock,
    )
    with pytest.raises(ValueError, match='phase'), operator.watch(phase, deadline_at=20.0):
        pass


@pytest.mark.parametrize('phase', ['internal', 'provider-query'])
def test_absolute_phase_heartbeat_does_not_slide_deadline(phase: WatchdogPhase) -> None:
    clock = _Clock()
    watchdog = ComponentWatchdog(
        kind='source',
        identity='events',
        worker_instance='spawn-1',
        registration_token=_CELL_IDENTITY,
        startup_deadline_secs=30.0,
        clock=clock,
    )

    with watchdog.watch(phase, deadline_at=20.0):
        clock._now = 17.0
        watchdog.heartbeat(sliding_deadline_secs=100.0)
        sample = watchdog.sample()

    assert sample.heartbeat_at == 17.0
    assert sample.deadline_at == 20.0


@pytest.mark.parametrize(
    'construct',
    [
        lambda: ComponentWatchdog(
            kind='source',
            identity='',
            worker_instance='spawn-1',
            registration_token=_CELL_IDENTITY,
            startup_deadline_secs=30.0,
        ),
        lambda: ComponentWatchdog(
            kind='source',
            identity='events',
            worker_instance='',
            registration_token=_CELL_IDENTITY,
            startup_deadline_secs=30.0,
        ),
        lambda: ComponentWatchdog(
            kind='source',
            identity='events',
            worker_instance='spawn-1',
            registration_token='',
            startup_deadline_secs=30.0,
        ),
        lambda: ComponentWatchdog(
            kind='source',
            identity='events',
            worker_instance='spawn-1',
            registration_token=_CELL_IDENTITY,
            startup_deadline_secs=0.0,
        ),
    ],
)
def test_watchdog_rejects_empty_identity_fields_and_nonpositive_startup_budget(
    construct: Callable[[], ComponentWatchdog],
) -> None:
    with pytest.raises(ValueError, match='must'):
        construct()
