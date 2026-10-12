"""Provide dependency-leaf worker component watchdog cells."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, replace
from math import isfinite
from typing import (
    TYPE_CHECKING,
    Literal,
    Protocol,
    TypeAlias,
    TypedDict,
    Unpack,
    get_args,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

GroupPhase: TypeAlias = Literal[
    'callback', 'idle', 'internal', 'setup', 'shutdown', 'startup', 'write-hold', 'write-lock-wait'
]
OperatorWatchdogPhase: TypeAlias = Literal['idle', 'internal', 'shutdown']
SourceWatchdogPhase: TypeAlias = Literal[
    'internal', 'provider-query', 'shutdown', 'startup', 'wait', 'write-hold', 'write-lock-wait'
]
WatchdogKind: TypeAlias = Literal['source', 'lane', 'operator']
WatchdogPhase: TypeAlias = GroupPhase | SourceWatchdogPhase | OperatorWatchdogPhase
GROUP_PHASES: tuple[GroupPhase, ...] = get_args(GroupPhase)
OPERATOR_WATCHDOG_PHASES: tuple[OperatorWatchdogPhase, ...] = get_args(OperatorWatchdogPhase)
SOURCE_WATCHDOG_PHASES: tuple[SourceWatchdogPhase, ...] = get_args(SourceWatchdogPhase)
WATCHDOG_KINDS: tuple[WatchdogKind, ...] = get_args(WatchdogKind)
WATCHDOG_PHASES: tuple[WatchdogPhase, ...] = tuple(
    sorted({*GROUP_PHASES, *SOURCE_WATCHDOG_PHASES, *OPERATOR_WATCHDOG_PHASES})
)
WATCHDOG_RULES: dict[WatchdogKind, tuple[tuple[WatchdogPhase, ...], WatchdogPhase]] = {
    'source': (SOURCE_WATCHDOG_PHASES, 'startup'),
    'lane': (GROUP_PHASES, 'startup'),
    'operator': (OPERATOR_WATCHDOG_PHASES, 'idle'),
}


class _ProviderBudget(Protocol):
    """Expose the provider retry settings used by watchdog deadlines."""

    @property
    def timeout_secs(self) -> float:
        """Return the maximum duration of one provider attempt."""
        ...

    @property
    def max_rpc_attempts(self) -> int:
        """Return the maximum number of provider attempts."""
        ...

    @property
    def rpc_attempt_interval_secs(self) -> float:
        """Return the delay between provider attempts."""
        ...


@dataclass(frozen=True)
class WatchdogSample:
    """Describe one immutable body-free component phase observation."""

    registration_token: str
    transition_sequence: int
    kind: WatchdogKind
    identity: str
    worker_instance: str
    phase: WatchdogPhase
    heartbeat_at: float
    deadline_at: float | None
    event_id: int | None = None
    plugin_id: str | None = None
    callback_id: str | None = None
    delivery_attempt: int | None = None
    operator_command: str | None = None


class _WatchIdentityOptions(TypedDict, total=False):
    """Type optional identities attached to one watchdog phase."""

    event_id: int | None
    plugin_id: str | None
    callback_id: str | None
    delivery_attempt: int | None
    operator_command: str | None


class ComponentWatchdog:
    """Publish one independently sampleable component phase."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        kind: WatchdogKind,
        identity: str,
        worker_instance: str,
        registration_token: str,
        startup_deadline_secs: float,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Create a bounded startup cell, or an idle operator without a deadline.

        Args:
            kind: Component role.
            identity: Component identity.
            worker_instance: Worker instance identity.
            registration_token: Registration identity.
            startup_deadline_secs: Positive startup budget in seconds; unused
                by operators.
            clock: Optional monotonic clock.

        Raises:
            ValueError: An identity is empty or the startup budget is invalid.
        """
        if not identity:
            raise ValueError('watchdog identity must not be empty')
        if not worker_instance:
            raise ValueError('watchdog worker instance must not be empty')
        if not registration_token:
            raise ValueError('watchdog registration token must not be empty')
        _validate_positive_float(startup_deadline_secs, 'startup deadline')
        self._clock = time.monotonic if clock is None else clock
        self._lock = threading.Lock()
        now = self._clock()
        phases, initial_phase = WATCHDOG_RULES[kind]
        if initial_phase not in phases:
            raise ValueError('watchdog phase is invalid for component kind')
        self._sample = WatchdogSample(
            registration_token=registration_token,
            transition_sequence=1,
            kind=kind,
            identity=identity,
            worker_instance=worker_instance,
            phase=initial_phase,
            heartbeat_at=now,
            deadline_at=now + startup_deadline_secs if initial_phase == 'startup' else None,
        )

    def _transition_locked(
        self,
        phase: WatchdogPhase,
        *,
        deadline_at: float | None,
        identities: _WatchIdentityOptions,
    ) -> None:
        """Replace the current phase while the caller owns the cell lock."""
        if phase not in WATCHDOG_RULES[self._sample.kind][0]:
            raise ValueError('watchdog phase is invalid for component kind')
        self._sample = replace(
            self._sample,
            transition_sequence=self._sample.transition_sequence + 1,
            phase=phase,
            heartbeat_at=self._clock(),
            deadline_at=deadline_at,
            event_id=identities.get('event_id'),
            plugin_id=identities.get('plugin_id'),
            callback_id=identities.get('callback_id'),
            delivery_attempt=identities.get('delivery_attempt'),
            operator_command=identities.get('operator_command'),
        )

    def heartbeat(self, *, sliding_deadline_secs: float | None = None) -> None:
        """Record owner progress and optionally slide callback/setup time.

        Args:
            sliding_deadline_secs: Positive plugin-operation budget. This
                slides only a current `callback` or `setup` deadline.

        Raises:
            ValueError: The provided sliding budget is invalid.
        """
        if sliding_deadline_secs is not None:
            _validate_positive_float(sliding_deadline_secs, 'sliding deadline')
        with self._lock:
            now = self._clock()
            deadline_at = self._sample.deadline_at
            if sliding_deadline_secs is not None and self._sample.phase in {
                'callback',
                'setup',
            }:
                deadline_at = now + sliding_deadline_secs
            self._sample = replace(
                self._sample,
                transition_sequence=self._sample.transition_sequence + 1,
                heartbeat_at=now,
                deadline_at=deadline_at,
            )

    def sample(self) -> WatchdogSample:
        """Return one immutable phase projection under the short cell lock.

        Returns:
            Current body-free component phase sample.
        """
        with self._lock:
            return self._sample

    def transition(
        self,
        phase: WatchdogPhase,
        *,
        deadline_at: float | None,
        **identities: Unpack[_WatchIdentityOptions],
    ) -> None:
        """Publish one persistent component phase transition.

        Args:
            phase: New component phase.
            deadline_at: Absolute monotonic deadline for the phase.
            **identities: Optional in-flight event, plugin, callback, delivery,
                or operator identities.

        Raises:
            ValueError: The phase is invalid for this component kind.
        """
        with self._lock:
            self._transition_locked(
                phase,
                deadline_at=deadline_at,
                identities=identities,
            )

    @contextmanager
    def watch(
        self,
        phase: WatchdogPhase,
        *,
        deadline_at: float | None,
        **identities: Unpack[_WatchIdentityOptions],
    ) -> Generator[None]:
        """Publish one watched phase while its operation runs.

        Args:
            phase: Scoped component phase.
            deadline_at: Absolute monotonic deadline for the phase.
            **identities: Optional in-flight event, plugin, callback, delivery,
                or operator identities.

        Yields:
            Control while the watched operation runs without the cell lock.

        Raises:
            ValueError: The phase is invalid for this component kind.
        """
        with self._lock:
            previous = self._sample
            self._transition_locked(
                phase,
                deadline_at=deadline_at,
                identities=identities,
            )
        try:
            yield
        finally:
            with self._lock:
                current = self._sample
                heartbeat_at = max(
                    previous.heartbeat_at,
                    current.heartbeat_at,
                    self._clock(),
                )
                deadline_at = previous.deadline_at
                if previous.phase == 'idle' and deadline_at is not None:
                    idle_budget = max(0.0, deadline_at - previous.heartbeat_at)
                    deadline_at = heartbeat_at + idle_budget
                self._sample = replace(
                    previous,
                    transition_sequence=current.transition_sequence + 1,
                    heartbeat_at=heartbeat_at,
                    deadline_at=deadline_at,
                )


def _validate_nonnegative_float(value: float, name: str) -> None:
    """Reject a non-finite or negative duration."""
    if type(value) not in {int, float} or not isfinite(value) or value < 0:
        raise ValueError(f'{name} must be finite and nonnegative')


def _validate_positive_float(value: float, name: str) -> None:
    """Reject a non-finite or nonpositive duration."""
    if type(value) not in {int, float} or not isfinite(value) or value <= 0:
        raise ValueError(f'{name} must be finite and positive')


def provider_budget_secs(credential: _ProviderBudget) -> float:
    """Return the nominal duration of all provider attempts and retry waits.

    Args:
        credential: Timeout, attempt count, and interval for provider calls.

    Raises:
        ValueError: A provider budget input is invalid.
    """
    _validate_positive_float(credential.timeout_secs, 'provider timeout')
    if type(credential.max_rpc_attempts) is not int or credential.max_rpc_attempts <= 0:
        raise ValueError('maximum RPC attempts must be a positive integer')
    _validate_nonnegative_float(credential.rpc_attempt_interval_secs, 'RPC attempt interval')
    return (
        credential.timeout_secs * credential.max_rpc_attempts
        + credential.rpc_attempt_interval_secs * (credential.max_rpc_attempts - 1)
    )


def internal_stall_deadline_secs(
    health_interval_secs: float,
    credential: _ProviderBudget,
) -> float:
    """Return the one shared internal-operation budget.

    Args:
        health_interval_secs: Monitor health publication interval.
        credential: Provider retry settings used for the nominal call budget.

    Returns:
        Internal-operation duration derived from health and provider budgets.

    Raises:
        ValueError: Any budget input is invalid.
    """
    _validate_positive_float(health_interval_secs, 'health interval')
    return 2 * max(5 * health_interval_secs, provider_budget_secs(credential))


def watchdog_grace_secs(health_interval_secs: float) -> float:
    """Return the five-health-interval allowance for idle and wait deadlines.

    Args:
        health_interval_secs: Worker health publication interval in seconds.
    """
    return 5 * health_interval_secs
