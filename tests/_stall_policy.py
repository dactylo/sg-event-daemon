from __future__ import annotations

import math
import sys
from contextlib import contextmanager
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from sg_event_daemon import _cli, _control, _lane, _supervisor, _watchdog, _worker, _write_lock
from sg_event_daemon._ipc import WorkerControl

if TYPE_CHECKING:
    from collections.abc import Generator

    from sg_event_daemon._config import Config
    from sg_event_daemon._ipc import ControlMessage
    from sg_event_daemon._watchdog import _ProviderBudget

STALL_DEADLINE_SECS = 3600.0
_CATEGORIES = frozenset({'startup', 'shutdown', 'heartbeat', 'phase', 'control', 'coordination'})
_ENABLED: frozenset[str] | None = None
_INTERNAL_DEADLINE = _watchdog.internal_stall_deadline_secs
_ORIGINAL_CONSTANTS = (
    (_supervisor, '_WORKER_STALE_INTERVALS', 'heartbeat', _supervisor._WORKER_STALE_INTERVALS),
    (_watchdog, '_WATCHDOG_GRACE_FLOOR_SECS', 'phase', _watchdog._WATCHDOG_GRACE_FLOOR_SECS),
    (_write_lock, 'WRITE_HOLD_DEADLINE_SECS', 'phase', _write_lock.WRITE_HOLD_DEADLINE_SECS),
    (
        _write_lock,
        'OPERATOR_WRITE_BUDGET_SECS',
        'coordination',
        _write_lock.OPERATOR_WRITE_BUDGET_SECS,
    ),
    (
        _supervisor,
        'OPERATOR_WRITE_BUDGET_SECS',
        'coordination',
        _supervisor.OPERATOR_WRITE_BUDGET_SECS,
    ),
    (
        _worker,
        '_LANE_OPERATOR_ADMISSION_SECS',
        'coordination',
        _worker._LANE_OPERATOR_ADMISSION_SECS,
    ),
    (_control, 'REQUEST_TIMEOUT_SECS', 'control', _control.REQUEST_TIMEOUT_SECS),
    (_control, 'OPERATOR_WAIT_CAP_SECS', 'control', _control.OPERATOR_WAIT_CAP_SECS),
    (_cli, 'REQUEST_TIMEOUT_SECS', 'control', _cli.REQUEST_TIMEOUT_SECS),
    (_cli, 'OPERATOR_WAIT_CAP_SECS', 'control', _cli.OPERATOR_WAIT_CAP_SECS),
)
_PRODUCTION_WRITE_HOLD = _write_lock.WRITE_HOLD_DEADLINE_SECS
_SEND_CONTROL = WorkerControl.send
_WRITE_ACQUISITION_TIMEOUT = _write_lock.write_lock_timeout_secs


def _write_acquisition_timeout(
    health_interval_secs: float, supervisor_poll_interval_secs: float
) -> float:
    # Nonterminal acquisition expiry yields source service or retries a group write.
    # Its cadence stays production even though a stalled holder's watchdog is lifted.
    budget = _WRITE_ACQUISITION_TIMEOUT(health_interval_secs, supervisor_poll_interval_secs)
    if _ENABLED is None or 'phase' in _ENABLED:
        return budget
    return budget - _write_lock.WRITE_HOLD_DEADLINE_SECS + _PRODUCTION_WRITE_HOLD


def active_stall_detectors() -> tuple[str, ...] | None:
    return None if _ENABLED is None else tuple(sorted(_ENABLED))


def stall_deadline(category: str, production: float) -> float:
    if category not in _CATEGORIES:
        raise ValueError(f'unknown stall detector category: {category}')
    if _ENABLED is None or category in _ENABLED:
        return production
    return max(STALL_DEADLINE_SECS, production)


def _internal_deadline(health_interval_secs: float, credential: _ProviderBudget) -> float:
    # This derived budget has no independent setting. Preserve provider retries and cadence.
    return stall_deadline('coordination', _INTERNAL_DEADLINE(health_interval_secs, credential))


def _send_control(
    control: WorkerControl, message: ControlMessage, *, deadline_secs: float
) -> None:
    # Send budgets are derived from heartbeat cadence at multiple callers.
    # Keep invalid inputs intact so validation remains observable.
    if math.isfinite(deadline_secs) and deadline_secs > 0:
        deadline_secs = stall_deadline('control', deadline_secs)
    _SEND_CONTROL(control, message, deadline_secs=deadline_secs)


def install_stall_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    if _ENABLED is None:
        return
    for module, name, category, production in _ORIGINAL_CONSTANTS:
        value = stall_deadline(category, production)
        if name == '_WORKER_STALE_INTERVALS' and value != production:
            # A multiplier cannot ensure a floor for every positive health interval.
            value = math.inf
        monkeypatch.setattr(module, name, value)
    for module in (_watchdog, _worker, _lane, _supervisor):
        monkeypatch.setattr(module, 'internal_stall_deadline_secs', _internal_deadline)
    for module in (_write_lock, _worker):
        monkeypatch.setattr(module, 'write_lock_timeout_secs', _write_acquisition_timeout)
    monkeypatch.setattr(WorkerControl, 'send', _send_control)


def activate_stall_policy(
    monkeypatch: pytest.MonkeyPatch, *, enabled: tuple[str, ...] | None = ()
) -> None:
    if enabled is None:
        return
    unknown = set(enabled) - _CATEGORIES
    if unknown:
        raise ValueError(f'unknown stall detector categories: {sorted(unknown)}')
    monkeypatch.setattr(sys.modules[__name__], '_ENABLED', frozenset(enabled))
    install_stall_policy(monkeypatch)


class StallDetectors:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._monkeypatch = monkeypatch

    def enable(self, *categories: str) -> None:
        unknown = set(categories) - _CATEGORIES
        if unknown:
            raise ValueError(f'unknown stall detector categories: {sorted(unknown)}')
        enabled = (_ENABLED or frozenset()) | set(categories)
        activate_stall_policy(self._monkeypatch, enabled=tuple(sorted(enabled)))

    @contextmanager
    def enabled(self, *categories: str) -> Generator[None]:
        with pytest.MonkeyPatch.context() as monkeypatch:
            detectors = StallDetectors(monkeypatch)
            detectors.enable(*categories)
            yield


def lift_config(config: Config) -> Config:
    if _ENABLED is None:
        return config
    daemon = config.daemon
    return replace(
        config,
        daemon=replace(
            daemon,
            worker_startup_deadline_secs=stall_deadline(
                'startup', daemon.worker_startup_deadline_secs
            ),
            shutdown_deadline_secs=stall_deadline('shutdown', daemon.shutdown_deadline_secs),
            async_cancel_grace_secs=stall_deadline('shutdown', daemon.async_cancel_grace_secs),
        ),
        sites={
            name: replace(
                site,
                groups={
                    group_name: replace(
                        group,
                        callback_deadline_secs=stall_deadline(
                            'phase', group.callback_deadline_secs
                        ),
                    )
                    for group_name, group in site.groups.items()
                },
            )
            for name, site in config.sites.items()
        },
    )
