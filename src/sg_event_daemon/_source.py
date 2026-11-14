"""Bounded source polling, catch-up reads, and token-fenced live joins."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from contextvars import copy_context
from dataclasses import dataclass, replace
from itertools import pairwise
from math import exp2, isfinite, log2
from threading import Event as ThreadEvent
from threading import Lock, current_thread
from typing import (
    TYPE_CHECKING,
    Literal,
    NotRequired,
    Protocol,
    TypeAlias,
    TypedDict,
    Unpack,
    cast,
    get_args,
)

from ._config import MIN_PENDING_EPOCHS
from ._events import EVENT_FIELDS, Event
from ._fanout import (
    EncodedEvent,
    EncoderHaltedError,
    EventEncoder,
    EventFanout,
    GroupChannelInterface,
)
from ._gaps import (
    clip_runs,
    log_catchup_skipped,
    log_gap_filled,
    log_ids_given_up,
    log_paused,
    log_resumed,
)
from ._group_errors import RevisionConflictError
from ._group_state import strict_capture_interval
from ._group_types import (
    LiveJoinIdentity,
)
from ._health import bound_health_text
from ._source_state import (
    SourceCommitResult,
    SourceIdentityError,
    SourcePause,
    SourceState,
)
from ._watchdog import SOURCE_WATCHDOG_PHASES, SourceWatchdogPhase
from ._write_lock import WriteLockTimeoutError, raise_owner_write_failure

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
    from contextlib import AbstractContextManager
    from types import TracebackType
    from typing import Self

    from ._config import CredentialConfig, SourceConfig
    from ._fanout import StrictOrderingControl
    from ._gaps import IdRun
    from ._group_state import GroupState, SourceProgress
    from ._group_types import CatchupEpoch, GroupSnapshot
    from ._providers import Provider, SyncSG
    from ._source_store import SourceSnapshot
    from ._strict import StrictBuffer
    from ._watchdog import ComponentWatchdog

_LOGGER = logging.getLogger(__name__)

_MAX_RETRY_DELAY_STEP_BITS = 64
_MAX_RUNS_PER_QUERY = 100

_StrictAuthority: TypeAlias = tuple[str, str, str, int, str, str]

SourceHaltStatus: TypeAlias = Literal[
    'encoder-failed',
    'event-too-large',
    'frame-too-large',
    'late-capacity-exhausted',
    'obligation-capacity-exhausted',
    'range-capacity-exhausted',
    'range-query-page-cap',
    'stream-identity-changed',
]
SourceRunStatus: TypeAlias = SourceHaltStatus | Literal['running', 'uninitialized']
HALTS_IN_PLACE: frozenset[SourceHaltStatus] = frozenset(
    {'encoder-failed', 'event-too-large', 'frame-too-large', 'stream-identity-changed'}
)
SOURCE_RUN_STATUSES: tuple[SourceRunStatus, ...] = tuple(
    status for status_group in get_args(SourceRunStatus) for status in get_args(status_group)
)
_SourceHaltCode: TypeAlias = SourceHaltStatus | Literal['commit-batch-capacity-exhausted']
_SOURCE_HALT_CODES: dict[str, SourceHaltStatus] = {
    'commit-batch-capacity-exhausted': 'frame-too-large',
    'encoder-failed': 'encoder-failed',
    'event-too-large': 'event-too-large',
    'frame-too-large': 'frame-too-large',
    'late-capacity-exhausted': 'late-capacity-exhausted',
    'obligation-capacity-exhausted': 'obligation-capacity-exhausted',
    'range-capacity-exhausted': 'range-capacity-exhausted',
    'range-query-page-cap': 'range-query-page-cap',
    'stream-identity-changed': 'stream-identity-changed',
}


@dataclass(frozen=True)
class _IdRunPage:
    """Carry one ascending provider page and the last requested ID it covers."""

    events: tuple[Event, ...]
    covered_through: int


@dataclass(frozen=True)
class Attached:
    """Acknowledge source-side provisional preparation only."""

    identity: LiveJoinIdentity


class HistoryTruncatedError(RuntimeError):
    """Report that retained SG history cannot satisfy catch-up.

    `retention_floor` is the lowest visible provider id, except that an empty
    visible history uses the synchronized `frontier + 1` as its effective
    floor.
    """

    def __init__(
        self,
        retention_floor: int,
        required_event_id: int,
        *,
        no_visible_rows: bool = False,
    ) -> None:
        """Store the effective floor and oldest required event identity.

        Args:
            retention_floor: Lowest usable event id, or the effective
                `frontier + 1` floor when no rows are visible.
            required_event_id: Oldest event id required by catch-up.
            no_visible_rows: Whether the floor represents empty visible history
                rather than an observed provider row.
        """
        self.retention_floor = retention_floor
        message = (
            f'history-truncated: no visible rows for required id {required_event_id}'
            if no_visible_rows
            else (
                f'history-truncated: retention floor {retention_floor} exceeds {required_event_id}'
            )
        )
        super().__init__(message)


@dataclass(frozen=True)
class JoinAbort:
    """Abort one exact provisional or joined attempt."""

    identity: LiveJoinIdentity


@dataclass(frozen=True)
class JoinAttemptIdentity:
    """Identify one caller-minted eager or strict join attempt."""

    site: str
    source: str
    group: str
    join_kind: Literal['eager', 'strict']
    group_generation: int
    attempt_token: str


@dataclass(frozen=True)
class JoinConfirm:
    """Confirm catch-up reached one exact captured boundary."""

    identity: LiveJoinIdentity


@dataclass(frozen=True)
class JoinFinalize:
    """Promote one exact prepared attempt after its durable compare-and-swap."""

    identity: LiveJoinIdentity


@dataclass(frozen=True)
class JoinFinalized:
    """Acknowledge exact source-side join promotion."""

    identity: LiveJoinIdentity


class JoinProtocolError(RuntimeError):
    """Report a stale, conflicting, or invalid live-join message."""


class LiveChannelDetachedError(JoinProtocolError):
    """Carry the original channel detach reason across owner cleanup.

    Attributes:
        reason: First recorded channel detach condition.
    """

    def __init__(self, reason: str) -> None:
        """Retain the detach condition observed before join settlement.

        Args:
            reason: Body-free channel detach condition.
        """
        super().__init__(f'live channel detached: {reason}')
        self.reason = reason


class JoinCapacityError(JoinProtocolError):
    """Report one exact strict attempt aborted by measured buffer capacity."""


@dataclass(frozen=True)
class JoinRequest:
    """Request one token-fenced source frontier capture."""

    attempt: JoinAttemptIdentity


@dataclass(frozen=True)
class JoinRetry:
    """Require a fresh-token join after bounded headroom is unavailable."""

    attempt: JoinAttemptIdentity
    retry_after_secs: float
    headroom_events: int
    headroom_bytes: int

    def __post_init__(self) -> None:
        """Reject invalid retry delays and headroom bounds at construction.

        Raises:
            ValueError: The delay is not an exact int or float with a finite,
                nonnegative value, or either headroom count is not an exact
                nonnegative integer.
        """
        delay = self.retry_after_secs
        if type(delay) not in {float, int} or delay < 0:
            raise ValueError('join retry delay is invalid')
        try:
            finite_delay = isfinite(delay)
        except OverflowError as exc:
            raise ValueError('join retry delay is invalid') from exc
        if not finite_delay:
            raise ValueError('join retry delay is invalid')
        if type(self.headroom_events) is not int or self.headroom_events < 0:
            raise ValueError('join retry event headroom is invalid')
        if type(self.headroom_bytes) is not int or self.headroom_bytes < 0:
            raise ValueError('join retry byte headroom is invalid')


@dataclass(frozen=True)
class JoinTarget:
    """Return one source-completed live-join target.

    The coordinator initially supplies only `identity`. A source-facing surface
    may add `stream_incarnation` and `source_successful_clock` from
    synchronized durable source state.
    """

    identity: LiveJoinIdentity
    stream_incarnation: str | None = None
    source_successful_clock: float | None = None


class SourceHaltedError(RuntimeError):
    """Report a source-local condition that forbids further polling.

    `code` identifies the halt independently of the diagnostic.
    """

    def __init__(self, message: str, *, code: _SourceHaltCode | None = None) -> None:
        """Keep the diagnostic separate from the machine-readable reason.

        Args:
            message: Human-readable diagnostic preserved by `str(error)`.
            code: Stable reason for control flow; `None` is unclassified.
        """
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class SourceHealth:
    """Describe one complete body-free source worker health projection.

    Each `strict_retention` row contains `(group, retention_floor,
    retained_event_count, retained_byte_count)`, counting payloads above that
    group's active release or join-lease floor.
    """

    site: str
    source: str
    source_generation: str
    status: SourceRunStatus
    frontier: int
    successful_clock: float
    retry_attempt: int
    next_retry_secs: float | None
    open_range_count: int
    late_id_count: int
    last_error: str | None
    worker_instance: str = ''
    initialized: bool = False
    pending_obligation_count: int = 0
    orphan_marker_count: int = 0
    strict_buffer_event_count: int = 0
    strict_buffer_byte_count: int = 0
    strict_retention: tuple[tuple[str, int, int, int], ...] = ()
    conditions: tuple[str, ...] = ()
    stream_incarnation: str | None = None
    audit_count: int = 0
    database_size_bytes: int = 0
    phase: SourceWatchdogPhase = 'wait'
    heartbeat_at: float = 0.0
    heartbeat_age: float = 0.0
    deadline_at: float | None = None
    overdue_reason: str | None = None

    def __post_init__(self) -> None:
        """Validate phase telemetry and bound source diagnostic text.

        Raises:
            ValueError: The phase is unsupported; a supplied heartbeat or
                deadline is not a finite nonnegative integer or float; or
                `audit_count` or `database_size_bytes` is negative.
        """
        if self.phase not in SOURCE_WATCHDOG_PHASES:
            raise ValueError('source health phase is invalid')
        if (
            type(self.heartbeat_at) not in {float, int}
            or not isfinite(self.heartbeat_at)
            or self.heartbeat_at < 0
        ):
            raise ValueError('source heartbeat must be finite and nonnegative')
        if (
            type(self.heartbeat_age) not in {float, int}
            or not isfinite(self.heartbeat_age)
            or self.heartbeat_age < 0
        ):
            raise ValueError('source heartbeat age must be finite and nonnegative')
        if self.deadline_at is not None and (
            type(self.deadline_at) not in {float, int}
            or not isfinite(self.deadline_at)
            or self.deadline_at < 0
        ):
            raise ValueError('source deadline must be finite and nonnegative')
        if self.audit_count < 0 or self.database_size_bytes < 0:
            raise ValueError('source telemetry scalars must be nonnegative')
        object.__setattr__(self, 'last_error', bound_health_text(self.last_error))
        object.__setattr__(self, 'overdue_reason', bound_health_text(self.overdue_reason))


@dataclass(frozen=True)
class _StrictCatchupResult:
    """Describe one bounded detached-strict epoch scheduling step."""

    source_generation: str
    stream_incarnation: str
    join_ready: bool
    exhausted_through: int | None
    exhausted_fence: tuple[str, int, int] | None
    exhausted_epoch_id: int | None


@dataclass(frozen=True)
class StrictDeliveryOutcome:
    """Describe durable page acceptance and same-epoch continuation.

    `consumed` counts the page prefix offered to the lane. `accepted` reports
    whether that prefix was durably accepted. `continue_epoch` independently
    permits offering the remaining suffix or the next page.
    """

    accepted: bool
    continue_epoch: bool
    consumed: int = 0


class _CatchupOptions(TypedDict):
    """Type one complete durable-state-refreshed catch-up pass."""

    cursor: int
    frontier: int
    delivery_floor: int
    source_progress: Callable[[], SourceProgress]
    deliver_page: Callable[[Sequence[Event]], None]
    between_pages: NotRequired[Callable[[], None]]
    unresolved_bounds: NotRequired[Sequence[tuple[int, int]]]
    replay_from: NotRequired[int | None]
    service: NotRequired[Callable[[float], None]]


@dataclass
class _JoinRecord:
    """Hold one in-memory token-fenced join phase."""

    attempt: JoinAttemptIdentity
    identity: LiveJoinIdentity
    channel: GroupChannelInterface
    initial: JoinTarget | JoinRetry
    phase: Literal['aborted', 'finalized', 'prepared', 'reserved']
    attached: Attached | None = None
    finalized: JoinFinalized | None = None
    requires_fresh: bool = False


class _StrictJoinRetention(Protocol):
    """Define the strict fixed-boundary retention injection."""

    def abort(self, identity: LiveJoinIdentity) -> None:
        """Remove one token-specific strict attempt.

        Args:
            identity: Complete exact attempt identity.
        """
        ...

    def acknowledge_control(
        self,
        control: StrictOrderingControl,
    ) -> None:
        """Retire one exact lane-committed strict control.

        Args:
            control: Full current live identity and committed decision.
        """
        ...

    def finalize(self, identity: LiveJoinIdentity) -> JoinFinalized:
        """Promote one matching strict provisional attempt.

        Args:
            identity: Complete exact attempt identity.

        Returns:
            Exact finalized acknowledgement.
        """
        ...

    def prepare(
        self,
        identity: LiveJoinIdentity,
        channel: GroupChannelInterface,
    ) -> Attached:
        """Prepare provisional strict queue, watermark, and lease state.

        Args:
            identity: Complete exact attempt identity.
            channel: Attempt-owned provisional group channel.

        Returns:
            Exact provisional attachment acknowledgement.
        """
        ...

    def reserve(self, identity: LiveJoinIdentity) -> JoinTarget | JoinRetry:
        """Reserve one fixed strict boundary.

        Args:
            identity: Complete source-captured live-join identity.

        Returns:
            Exact target or bounded fresh-token retry.
        """
        ...


class _SuccessfulQueryClock:
    """Accumulate elapsed process time while excluding failed-query outages."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        initial_value: float = 0.0,
    ) -> None:
        """Create one monotonic successful-query clock."""
        if initial_value < 0:
            raise ValueError('successful-query clock must be nonnegative')
        self._clock = clock
        self._recovering = False
        self._last_observation = clock()
        self._successful_value = float(initial_value)

    def failed(self) -> float:
        """Pause accumulation across one failed query.

        Returns:
            Accumulated successful-query seconds, unchanged by this failed
            query.

        Raises:
            RuntimeError: The supplied monotonic clock moves backward.
        """
        now = self._clock()
        if now < self._last_observation:
            raise RuntimeError('successful-query wall clock moved backward')
        self._recovering = True
        self._last_observation = now
        return self._successful_value

    def resume_from(self, checkpoint: float) -> float:
        """Adopt a durable checkpoint without counting process downtime.

        Args:
            checkpoint: Finite nonnegative successful-query time in seconds.

        Returns:
            The greater of the current accumulated time and the checkpoint.

        Raises:
            ValueError: The checkpoint is negative or non-finite.
        """
        if not isfinite(checkpoint) or checkpoint < 0:
            raise ValueError('successful-query checkpoint must be finite and nonnegative')
        self._successful_value = max(self._successful_value, checkpoint)
        return self._successful_value

    def succeeded(self) -> float:
        """Advance successful-query time except across the first recovery interval.

        Returns:
            Accumulated successful-query seconds. The first success after a
            failure ends recovery without adding the intervening elapsed time.

        Raises:
            RuntimeError: The supplied monotonic clock moves backward.
        """
        now = self._clock()
        if now < self._last_observation:
            raise RuntimeError('successful-query wall clock moved backward')
        if self._recovering:
            self._recovering = False
        else:
            self._successful_value += now - self._last_observation
        self._last_observation = now
        return self._successful_value

    @property
    def value(self) -> float:
        """Return accumulated successful-query time."""
        return self._successful_value


class JoinCoordinator:
    """Coordinate provisional eager and fixed-boundary strict joins.

    The coordinator is scoped to one source generation and permits one current
    attempt per group. Exact current-token replies are idempotent; a new
    attempt retires superseded terminal history, while stale or conflicting
    identities fail.
    """

    def __init__(
        self,
        *,
        state: SourceState,
        fanout: EventFanout,
        source_generation: str,
        strict_retention: _StrictJoinRetention | None = None,
    ) -> None:
        """Create one source-generation-scoped join coordinator.

        Args:
            state: Sole durable source-state authority.
            fanout: Synchronized eager fanout registry.
            source_generation: Supervisor-issued source process identity.
            strict_retention: Injected strict fixed-boundary protocol.
        """
        self._active: dict[str, _JoinRecord] = {}
        self._fanout = fanout
        self._records: dict[str, _JoinRecord] = {}
        self._source_generation = source_generation
        self._state = state
        self._strict_retention = strict_retention

    def _live_identity(self, attempt: JoinAttemptIdentity) -> LiveJoinIdentity:
        """Capture the current durable frontier for one attempt."""
        snapshot = self._state.snapshot()
        return LiveJoinIdentity(
            site=attempt.site,
            source=attempt.source,
            group=attempt.group,
            join_kind=attempt.join_kind,
            source_generation=self._source_generation,
            group_generation=attempt.group_generation,
            boundary=snapshot.frontier,
            attempt_token=attempt.attempt_token,
        )

    def _matching(self, identity: LiveJoinIdentity) -> _JoinRecord:
        """Return the exact current record or reject a stale message."""
        record = self._records.get(identity.attempt_token)
        if record is None or record.identity != identity:
            raise JoinProtocolError('stale join attempt')
        if self._active.get(identity.group) is not record:
            raise JoinProtocolError('stale join attempt')
        if identity.source_generation != self._source_generation:
            raise JoinProtocolError('stale source generation')
        return record

    def _require_strict_retention(self) -> _StrictJoinRetention:
        """Return the configured strict-retention authority."""
        retention = self._strict_retention
        if retention is None:
            raise JoinProtocolError('strict join retention is unavailable')
        return retention

    def _abort_record(self, record: _JoinRecord, reason: str) -> None:
        """Abort one exact current record without touching a newer token."""
        if record.phase == 'aborted':
            record.requires_fresh = True
            return
        if record.attempt.join_kind == 'strict':
            self._require_strict_retention().abort(record.identity)
        else:
            current = self._fanout.channel(record.attempt.group)
            if current is record.channel:
                self._fanout.detach(record.attempt.group, reason=reason)
        record.phase = 'aborted'
        record.requires_fresh = True
        self._active.pop(record.attempt.group, None)

    def _retire_terminal_history(self, group: str) -> None:
        """Discard the superseded terminal token for one group."""
        for token, previous in tuple(self._records.items()):
            if previous.attempt.group != group:
                continue
            if previous.phase not in {'aborted', 'finalized'}:
                raise JoinProtocolError('group join history is not terminal')
            del self._records[token]

    @staticmethod
    def _retry(record: _JoinRecord) -> JoinRetry:
        """Build one deterministic fresh-token retry response."""
        return JoinRetry(
            attempt=record.attempt,
            retry_after_secs=0.0,
            headroom_events=0,
            headroom_bytes=0,
        )

    @staticmethod
    def _validate_attempt(
        attempt: JoinAttemptIdentity,
        channel: GroupChannelInterface,
    ) -> None:
        """Reject malformed or channel-conflicting attempt identities."""
        if not attempt.site or not attempt.source or not attempt.group:
            raise JoinProtocolError('join identity fields must not be empty')
        if not attempt.attempt_token:
            raise JoinProtocolError('join attempt token must not be empty')
        if attempt.group_generation < 0:
            raise JoinProtocolError('join group generation must be nonnegative')
        if attempt.join_kind not in {'eager', 'strict'}:
            raise JoinProtocolError('unsupported join kind')
        if channel.group != attempt.group or channel.ordering != attempt.join_kind:
            raise JoinProtocolError('join channel identity conflicts')

    @staticmethod
    def _validate_retry(
        attempt: JoinAttemptIdentity,
        retry: JoinRetry,
    ) -> None:
        """Reject a strict retry for a foreign attempt."""
        if retry.attempt != attempt:
            raise JoinProtocolError('strict retry attempt identity conflicts')

    def abort(self, abort: JoinAbort) -> None:
        """Abort one exact attempt idempotently and token-specifically.

        Args:
            abort: Complete captured live-join identity.
        """
        record = self._records.get(abort.identity.attempt_token)
        if record is None or record.identity != abort.identity:
            return
        if self._active.get(abort.identity.group) is not record:
            return
        self._abort_record(record, 'join-aborted')

    def acknowledge_strict_control(
        self,
        control: StrictOrderingControl,
    ) -> None:
        """Retire one exact strict control after the configured-level lane commit.

        Args:
            control: Full finalized live identity and committed decision.

        Raises:
            JoinProtocolError: The control identity is stale or conflicting,
                the join phase does not permit acknowledgement, or the retained
                control was not offered.
        """
        identity = control.identity
        record = self._records.get(identity.attempt_token)
        if (
            record is None
            or record.identity != identity
            or identity.source_generation != self._source_generation
        ):
            raise JoinProtocolError('stale strict control acknowledgement')
        if record.attempt.join_kind != 'strict' or record.phase not in {
            'aborted',
            'finalized',
        }:
            raise JoinProtocolError('strict control acknowledgement is not finalized')
        self._require_strict_retention().acknowledge_control(control)

    def confirm(self, confirmation: JoinConfirm) -> Attached | JoinRetry:
        """Prepare one exact attempt after catch-up reaches its boundary.

        The caller must complete catch-up to the captured boundary before
        confirmation. After attachment it must durably commit the exact group
        join before calling `finalize`.

        Args:
            confirmation: Complete captured live-join identity.

        Returns:
            Provisional attachment or fresh-token retry.

        Raises:
            JoinProtocolError: The identity is stale or conflicts with the
                current attempt.
            ValueError: A different attached channel already owns the group.
            OSError: Closing the remote channel after a strict-capacity retry
                fails.
        """
        record = self._matching(confirmation.identity)
        if record.phase == 'prepared' and record.attached is not None:
            return record.attached
        if record.phase == 'finalized' and record.attached is not None:
            return record.attached
        if record.attempt.join_kind == 'eager':
            if not record.channel.status.attached:
                record.phase = 'aborted'
                record.requires_fresh = True
                self._active.pop(record.attempt.group, None)
                return self._retry(record)
            attached = Attached(record.identity)
        else:
            retention = self._require_strict_retention()
            self._fanout.attach(record.channel, provisional=True)
            try:
                attached = retention.prepare(
                    record.identity,
                    record.channel,
                )
            except JoinCapacityError:
                self._fanout.detach(
                    record.attempt.group,
                    reason='strict-buffer-capacity',
                )
                record.phase = 'aborted'
                record.requires_fresh = True
                self._active.pop(record.attempt.group, None)
                return self._retry(record)
            if attached != Attached(record.identity):
                raise JoinProtocolError('strict prepare acknowledgement conflicts')
        record.attached = attached
        record.phase = 'prepared'
        return attached

    def finalize(self, finalization: JoinFinalize) -> JoinFinalized:
        """Promote one exact prepared attempt idempotently.

        The caller must first durably commit the exact group-side join after
        confirmation. Only the matching current prepared token can be promoted;
        an exact finalized retry returns its retained result.

        Args:
            finalization: Complete captured live-join identity.

        Returns:
            Exact finalized acknowledgement.

        Raises:
            JoinProtocolError: The identity is stale, conflicting, or not
                prepared.
            LiveChannelDetachedError: The channel detached before promotion;
                the source attempt is aborted and a fresh join is required.
            ValueError: Another attempt owns the registered fanout channel.
        """
        existing = self._records.get(finalization.identity.attempt_token)
        if (
            existing is not None
            and existing.identity == finalization.identity
            and existing.finalized is not None
            and existing.phase == 'finalized'
            and not existing.requires_fresh
        ):
            return existing.finalized
        record = self._matching(finalization.identity)
        if record.phase != 'prepared':
            raise JoinProtocolError('join attempt is not prepared')
        retention = None
        if record.attempt.join_kind == 'strict':
            retention = self._require_strict_retention()
            finalized = retention.finalize(record.identity)
            if finalized != JoinFinalized(record.identity):
                raise JoinProtocolError('strict finalize acknowledgement conflicts')
        else:
            finalized = JoinFinalized(record.identity)
        try:
            self._fanout.promote(
                record.attempt.group,
                record.channel,
            )
        except BaseException as error:
            if isinstance(error, (EncoderHaltedError, ConnectionError)):
                status = record.channel.status
                if not status.attached:
                    reason = cast('str', status.reason)
                    self._abort_record(record, reason)
                    raise LiveChannelDetachedError(reason) from error
            if retention is not None:
                retention.abort(record.identity)
            raise
        record.finalized = finalized
        record.phase = 'finalized'
        return finalized

    def group_died(self, group: str, group_generation: int) -> None:
        """Abort active attempts for one exact dead group generation.

        Args:
            group: Stable group identity.
            group_generation: Exact durable generation.
        """
        record = self._active.get(group)
        if record is not None and record.attempt.group_generation == group_generation:
            self._abort_record(record, 'group-died')

    def request(
        self,
        request: JoinRequest,
        channel: GroupChannelInterface,
    ) -> JoinTarget | JoinRetry:
        """Capture or replay one exact token-fenced join request.

        Args:
            request: Caller-minted non-reusable attempt.
            channel: Attempt-owned bounded group channel.

        Returns:
            Exact captured target or bounded retry.

        Raises:
            JoinProtocolError: The token is stale, reused, or conflicts.
            sqlite3.Error: Reading durable source state fails.
            RuntimeError: Source metadata is missing or its persisted condition
                is unknown.
            ValueError: A different attached channel already owns the group.
        """
        attempt = request.attempt
        self._validate_attempt(attempt, channel)
        snapshot = self._state.snapshot()
        if attempt.site != snapshot.site or attempt.source != snapshot.source:
            raise JoinProtocolError('join source identity conflicts')
        recorded = self._records.get(attempt.attempt_token)
        if recorded is not None:
            if recorded.attempt != attempt:
                raise JoinProtocolError('join attempt token was reused')
            if recorded.requires_fresh:
                raise JoinProtocolError('join retry requires a fresh attempt token')
            return recorded.initial
        current = self._active.get(attempt.group)
        if current is not None:
            raise JoinProtocolError('group already has an active join attempt')
        self._retire_terminal_history(attempt.group)
        with self._fanout.locked():
            identity = self._live_identity(attempt)
            target = JoinTarget(identity)
            if attempt.join_kind == 'strict':
                initial = self._require_strict_retention().reserve(identity)
                if isinstance(initial, JoinTarget) and initial.identity != identity:
                    raise JoinProtocolError('strict retention target conflicts')
                if isinstance(initial, JoinRetry):
                    self._validate_retry(attempt, initial)
            else:
                self._fanout.attach(channel, provisional=True)
                initial = target
        record = _JoinRecord(
            attempt=attempt,
            identity=identity,
            channel=channel,
            initial=initial,
            phase='reserved',
        )
        self._records[attempt.attempt_token] = record
        if isinstance(initial, JoinTarget):
            self._active[attempt.group] = record
        else:
            record.phase = 'aborted'
        return initial


class _StrictCatchupOptions(TypedDict):
    """Type one bounded detached-strict scheduling step."""

    state: GroupState
    source_progress: Callable[[], SourceProgress]
    deliver: Callable[[Sequence[Event], float], StrictDeliveryOutcome]
    holdback_secs: float
    max_pending_epochs: int
    unresolved_bounds: NotRequired[Sequence[tuple[int, int]]]
    join_boundary: NotRequired[int | None]
    service: NotRequired[Callable[[float], None]]


@dataclass(frozen=True)
class _StrictStepContext:
    """Hold one validated detached-strict scheduling decision."""

    deliver: Callable[[Sequence[Event], float], StrictDeliveryOutcome]
    holdback_secs: float
    interval: float
    join_boundary: int | None
    max_pending_epochs: int
    reader_clock: float
    state: GroupState
    synchronization: SourceProgress
    target: int
    unresolved_bounds: Sequence[tuple[int, int]]
    obligation_fence: tuple[str, int, int] | None
    service: Callable[[float], None] | None


@dataclass(frozen=True)
class _StrictPreparedPage:
    """Retain one provider page and its immutable query evidence for one step."""

    events: tuple[Event, ...]
    authority: _StrictAuthority
    epoch: CatchupEpoch
    fence: tuple[str, int, int] | None
    query_high: int


class _Waiter(Protocol):
    """Define the interruptible waits injected into source readers."""

    def set(self) -> None:
        """Interrupt current and future waits."""
        ...

    def wait(self, seconds: float) -> bool:
        """Wait up to the requested interval.

        Args:
            seconds: Maximum interval to wait.

        Returns:
            `True` when interrupted.
        """
        ...


class _CatchupReaderOptions(TypedDict):
    """Type one complete catch-up reader construction request."""

    provider: Provider
    site_url: str
    credential: CredentialConfig
    batch_size: int
    fetch_interval_secs: float
    waiter: _Waiter
    log_extra: NotRequired[Mapping[str, str]]
    provider_watch: NotRequired[Callable[[], AbstractContextManager[object]] | None]
    successful_clock: NotRequired[_SuccessfulQueryClock | None]


class CatchupReader:
    """Own an independent provider handle for bounded historical reads.

    The handle is created at context entry and remains owned until `close()` or
    context exit. Query operations require that open lifetime.
    """

    def __init__(
        self,
        **options: Unpack[_CatchupReaderOptions],
    ) -> None:
        """Configure one independent catch-up reader.

        Args:
            options: Provider identity, bounded page policy, and interruptible
                wait dependency.

        Raises:
            ValueError: The batch size or fetch interval is not positive.
        """
        batch_size = options['batch_size']
        fetch_interval_secs = options['fetch_interval_secs']
        if batch_size <= 0 or fetch_interval_secs <= 0:
            raise ValueError('catch-up limits must be positive')
        self._batch_size = batch_size
        self._credential = options['credential']
        self._fetch_interval_secs = fetch_interval_secs
        self._handle: SyncSG | None = None
        self._latest_strict_capture_at: float | None = None
        self._latest_strict_capture_bound: int | None = None
        self._log_extra = options.get('log_extra', {})
        self._provider = options['provider']
        self._provider_watch = options.get('provider_watch')
        self._site_url = options['site_url']
        successful_clock = options.get('successful_clock')
        self._successful_clock = (
            _SuccessfulQueryClock() if successful_clock is None else successful_clock
        )
        self._waiter = options['waiter']

    def __enter__(self) -> Self:
        """Open the reader's independent provider handle.

        Returns:
            This live catch-up reader.

        Raises:
            Exception: Creating or configuring the supplied provider handle
                fails.
        """
        if self._handle is None:
            with self._provider_watch() if self._provider_watch is not None else nullcontext():
                self._handle = self._provider.create_sync(
                    self._site_url,
                    self._credential,
                )
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Close the reader at context exit.

        Raises:
            Exception: Closing the supplied provider handle fails.
        """
        self.close()

    def _capture_strict_epoch(
        self,
        context: _StrictStepContext,
        snapshot: GroupSnapshot,
    ) -> GroupSnapshot:
        """Capture one due disjoint epoch and remember its frontier evidence."""
        pending = snapshot.epochs
        if pending:
            latest = pending[-1]
            if (
                self._latest_strict_capture_at is None
                or latest.captured_at >= self._latest_strict_capture_at
            ):
                self._latest_strict_capture_at = latest.captured_at
                self._latest_strict_capture_bound = latest.high_id
        low_id = snapshot.cursor + 1 if not pending else pending[-1].high_id + 1
        spaced = not pending or (
            context.reader_clock >= pending[-1].captured_at + context.interval
        )
        if len(pending) >= context.max_pending_epochs or not spaced or context.target < low_id:
            return snapshot
        captured = context.state.capture_epoch(
            low_id=low_id,
            high_id=context.target,
            captured_at=context.reader_clock,
            eligibility_point=context.reader_clock + context.holdback_secs,
            reader_clock_checkpoint=context.reader_clock,
            source_generation=snapshot.source_generation,
            expected_revision=snapshot.revision,
        )
        self._latest_strict_capture_at = context.reader_clock
        self._latest_strict_capture_bound = context.target
        return captured

    @staticmethod
    def _check_floor(floor: int | None, required: int | None, frontier: int) -> None:
        """Reject a floor that proves required history was truncated."""
        error = history_truncation(floor, required, frontier)
        if error is not None:
            raise error

    def _checkpoint_strict_reader(
        self,
        context: _StrictStepContext,
        snapshot: GroupSnapshot,
    ) -> GroupSnapshot:
        """Persist due reader-clock progress while any epoch remains."""
        reader_clock = self._successful_clock.value
        if (
            not snapshot.epochs
            or reader_clock < snapshot.reader_clock_checkpoint + context.interval
        ):
            return snapshot
        return context.state.checkpoint_reader_clock(
            reader_clock,
            source_generation=snapshot.source_generation,
            expected_revision=snapshot.revision,
        )

    @staticmethod
    def _deliver_page(events: Sequence[Event], options: _CatchupOptions) -> None:
        """Deliver the bounded prefix above the durable delivery floor."""
        eligible = tuple(
            event for event in events if cast('int', event['id']) > options['delivery_floor']
        )
        if eligible:
            options['deliver_page'](eligible)

    @staticmethod
    def _events(
        rows: Sequence[Mapping[str, object]],
    ) -> tuple[Event, ...]:
        """Validate one provider page as strictly ascending events."""
        events = tuple(Event(row) for row in rows)
        ids = tuple(cast('int', event['id']) for event in events)
        if any(type(event_id) is not int or event_id <= 0 for event_id in ids):
            raise ValueError('catch-up event id is invalid')
        if any(left >= right for left, right in pairwise(ids)):
            raise ValueError('catch-up provider page is not strictly ascending')
        return events

    @contextmanager
    def _query(self) -> Generator[None]:
        """Record one provider operation's clock outcome at its owner's boundary."""
        with self._provider_watch() if self._provider_watch is not None else nullcontext():
            try:
                yield
            except BaseException:
                self._successful_clock.failed()
                raise
            self._successful_clock.succeeded()

    def _latest_visible_id(self, handle: SyncSG) -> int | None:
        """Query and validate one liveness-probe frontier."""
        with self._query():
            return edge_visible_id(handle, 'desc')

    def _lowest_visible_id(self, handle: SyncSG) -> int | None:
        """Query and validate the current retention floor."""
        with self._query():
            return edge_visible_id(handle, 'asc')

    @staticmethod
    def _oldest_required(
        *,
        cursor: int,
        delivery_floor: int,
        target: int,
        unresolved_bounds: Sequence[tuple[int, int]],
        replay_from: int | None,
    ) -> int | None:
        """Return the lowest position that must remain queryable."""
        required = [
            max(low_id, delivery_floor + 1)
            for low_id, high_id in unresolved_bounds
            if high_id > delivery_floor
        ]
        if replay_from is not None:
            required.append(max(replay_from, delivery_floor + 1))
        elif cursor < target and delivery_floor < target:
            required.append(max(cursor + 1, delivery_floor + 1))
        return None if not required else min(required)

    def _query_range(
        self,
        handle: SyncSG,
        low_id: int,
        high_id: int,
        *,
        inclusive_low: bool,
    ) -> list[dict[str, object]]:
        """Query one ordered bounded EventLogEntry interval."""
        low_boundary = low_id - 1 if inclusive_low else low_id
        with self._query():
            return handle.find(
                'EventLogEntry',
                [
                    ['id', 'greater_than', low_boundary],
                    ['id', 'less_than', high_id + 1],
                ],
                list(EVENT_FIELDS),
                limit=self._batch_size,
                order=[{'field_name': 'id', 'direction': 'asc'}],
                page=0,
            )

    def _query_events(
        self,
        handle: SyncSG,
        cursor: int,
        target: int,
        *,
        inclusive_low: bool,
    ) -> tuple[int, tuple[Event, ...], bool]:
        """Read and validate one bounded provider page."""
        rows = self._query_range(
            handle,
            cursor,
            target,
            inclusive_low=inclusive_low,
        )
        page_events = self._events(rows)
        if page_events:
            first_id = cast('int', page_events[0]['id'])
            page_cursor = cast('int', page_events[-1]['id'])
            if (
                first_id < cursor
                or (first_id == cursor and not inclusive_low)
                or page_cursor > target
            ):
                raise ValueError('catch-up provider page is outside the requested range')
        else:
            page_cursor = cursor
        return page_cursor, page_events, len(rows) < self._batch_size

    def _require_open(self) -> SyncSG:
        """Return the live provider handle."""
        if self._handle is None:
            raise RuntimeError('catch-up reader is not open')
        return self._handle

    def _service_reader(
        self,
        service: Callable[[float], None] | None,
        seconds: float = 0.0,
    ) -> None:
        """Service owner work or wait interruptibly between provider calls."""
        if service is not None:
            service(seconds)
        elif seconds and self._waiter.wait(seconds):
            raise InterruptedError('catch-up interrupted')

    def _query_gap_page(
        self,
        handle: SyncSG,
        cursor: int,
        target: int,
        gap_target: int | None,
        service: Callable[[float], None] | None,
    ) -> tuple[int, int | None, tuple[Event, ...]]:
        """Return the contiguous prefix or floor-checked two-read gap progress."""
        query_target = target if gap_target is None else gap_target
        inclusive_low = gap_target is not None
        query_cursor = cursor + 1 if inclusive_low else cursor
        _page_cursor, events, exhausted = self._query_events(
            handle,
            query_cursor,
            query_target,
            inclusive_low=inclusive_low,
        )
        self._service_reader(service)
        if not events:
            if gap_target is None:
                return cursor, target, ()
            floor = self._lowest_visible_id(handle)
            self._service_reader(service)
            self._check_floor(floor, cursor + 1, query_target)
            return query_target, None, ()
        expected = cursor + 1
        for index, event in enumerate(events):
            event_id = cast('int', event['id'])
            if event_id > expected:
                return cursor, event_id - 1, events[:index]
            cursor = event_id
            expected = event_id + 1
        if cursor >= query_target:
            return cursor, None, events
        if exhausted:
            return cursor, query_target, events
        return cursor, gap_target, events

    def _consume_ordinary_page(
        self,
        handle: SyncSG,
        cursor: int,
        target: int,
        gap_target: int | None,
        options: _CatchupOptions,
    ) -> tuple[int, int | None]:
        """Deliver the contiguous prefix before discarding and re-querying a gap."""
        page_cursor, gap_target, events = self._query_gap_page(
            handle,
            cursor,
            target,
            gap_target,
            options.get('service'),
        )
        self._deliver_page(events, options)
        if not events and page_cursor > cursor:
            log_catchup_skipped(
                self._log_extra, cursor + 1, page_cursor, self._fetch_interval_secs
            )
        return page_cursor, gap_target

    def _service_catchup(self, options: _CatchupOptions, seconds: float = 0.0) -> None:
        """Service eager owner work with the reader's interruptible fallback."""
        self._service_reader(options.get('service'), seconds)

    def _read_ordinary(
        self,
        handle: SyncSG,
        target: int,
        options: _CatchupOptions,
    ) -> None:
        """Read ordinary pages with discard-and-refetch gap checks."""
        cursor = options['cursor']
        gap_target: int | None = None
        while cursor < target:
            if (between_pages := options.get('between_pages')) is not None:
                between_pages()
            if gap_target is not None:
                self._service_catchup(options, self._fetch_interval_secs)
            cursor, gap_target = self._consume_ordinary_page(
                handle,
                cursor,
                target,
                gap_target,
                options,
            )

    def _read_replay(
        self,
        handle: SyncSG,
        target: int,
        options: _CatchupOptions,
        replay_from: int,
    ) -> None:
        """Read replay pages while leaving the ordinary cursor unchanged."""
        cursor = max(replay_from - 1, options['delivery_floor'])
        self._read_ordinary(handle, target, {**options, 'cursor': cursor})

    def _read_and_deliver(
        self,
        handle: SyncSG,
        target: int,
        options: _CatchupOptions,
    ) -> None:
        """Read every bounded page without retaining it across fetches."""
        replay_from = options.get('replay_from')
        if replay_from is None:
            self._read_ordinary(handle, target, options)
            return
        self._read_replay(handle, target, options, replay_from)

    @staticmethod
    def _service_strict(options: _StrictCatchupOptions) -> GroupSnapshot:
        """Return fresh durable authority after any due owner work."""
        service = options.get('service')
        if service is not None:
            service(0.0)
        return options['state'].snapshot()

    @staticmethod
    def _strict_authority(snapshot: GroupSnapshot) -> _StrictAuthority:
        """Capture identity evidence without retaining mutation authority."""
        return (
            snapshot.site,
            snapshot.source,
            snapshot.group,
            snapshot.group_generation,
            snapshot.source_generation,
            snapshot.verified_incarnation,
        )

    @staticmethod
    def _strict_delivery_ceiling(
        context: _StrictStepContext,
        epoch: CatchupEpoch,
    ) -> int:
        """Return the safe prefix ceiling or a body-free stall sentinel."""
        if context.synchronization.frontier < epoch.high_id:
            return epoch.low_id - 1
        lower_bounds = (
            low_id
            for low_id, high_id in context.unresolved_bounds
            if high_id >= epoch.low_id and low_id <= epoch.high_id
        )
        first_unresolved = min(lower_bounds, default=None)
        fence = context.obligation_fence
        if fence is not None and fence[1] <= epoch.high_id:
            first_unresolved = (
                fence[1] if first_unresolved is None else min(first_unresolved, fence[1])
            )
        if first_unresolved is None:
            return epoch.high_id
        return min(epoch.high_id, first_unresolved - 1)

    def _deliver_strict_page(
        self,
        context: _StrictStepContext,
        page: _StrictPreparedPage,
    ) -> bool:
        """Accept prepared prefixes only after fresh post-service authority reads."""
        remaining = page.events
        while True:
            if context.service is not None:
                context.service(0.0)
            current = context.state.snapshot()
            fence = context.state.strict_obligation_fence()
            if (
                self._strict_authority(current) != page.authority
                or current.status != 'running'
                or not current.epochs
                or current.epochs[0] != page.epoch
                or fence != page.fence
            ):
                return False
            fresh = replace(
                context,
                reader_clock=self._successful_clock.value,
                unresolved_bounds=(
                    *context.unresolved_bounds,
                    *context.state.unresolved_obligation_bounds(),
                ),
                obligation_fence=fence,
            )
            if self._strict_delivery_ceiling(fresh, page.epoch) < page.query_high:
                return False
            remaining = tuple(
                event for event in remaining if cast('int', event['id']) > current.cursor
            )
            if not remaining:
                return True
            outcome = fresh.deliver(remaining, page.epoch.captured_at)
            if not 0 <= outcome.consumed <= len(remaining):
                raise ValueError('strict delivery consumed invalid page prefix')
            if not outcome.accepted or not outcome.continue_epoch or outcome.consumed == 0:
                return False
            remaining = remaining[outcome.consumed :]

    def _deliver_strict_epoch(
        self,
        context: _StrictStepContext,
        snapshot: GroupSnapshot,
        handle: SyncSG,
    ) -> tuple[GroupSnapshot, bool]:
        """Prepare bounded pages, then accept them under fresh durable authority."""
        if not snapshot.epochs or snapshot.epochs[0].state != 'delivering':
            return snapshot, False
        epoch = snapshot.epochs[0]
        if context.synchronization.frontier < epoch.high_id or snapshot.conditions:
            return snapshot, False
        delivery_ceiling = self._strict_delivery_ceiling(context, epoch)
        cursor = max(epoch.low_id - 1, snapshot.cursor)
        gap_target: int | None = None
        while cursor < delivery_ceiling:
            if gap_target is not None:
                self._service_reader(context.service, self._fetch_interval_secs)
            page_cursor, gap_target, events = self._query_gap_page(
                handle,
                cursor,
                delivery_ceiling,
                gap_target,
                context.service,
            )
            page = _StrictPreparedPage(
                events=events,
                authority=self._strict_authority(snapshot),
                epoch=epoch,
                fence=context.obligation_fence,
                query_high=delivery_ceiling,
            )
            if not self._deliver_strict_page(context, page):
                current = context.state.snapshot()
                if (
                    self._strict_authority(current) == page.authority
                    and current.epochs
                    and current.epochs[0] == page.epoch
                    and context.state.strict_obligation_fence() == page.fence
                ):
                    current = self._checkpoint_strict_reader(context, current)
                return current, False
            if not events and page_cursor > cursor:
                log_catchup_skipped(
                    self._log_extra,
                    cursor + 1,
                    page_cursor,
                    self._fetch_interval_secs,
                )
            cursor = page_cursor
            del page, events
        current = context.state.snapshot()
        exhausted = not current.conditions and current.required_event_id is None
        if delivery_ceiling >= epoch.high_id and exhausted:
            current = context.state.complete_epoch(
                epoch.epoch_id,
                epoch.high_id,
                expected_revision=current.revision,
            )
        return self._checkpoint_strict_reader(context, current), exhausted

    def _strict_join_ready(
        self,
        context: _StrictStepContext,
        snapshot: GroupSnapshot,
    ) -> bool:
        """Return whether catch-up may request or finish a fixed join."""
        if snapshot.epochs:
            return False
        if context.join_boundary is not None:
            return snapshot.cursor >= context.join_boundary
        if context.synchronization.frontier <= snapshot.cursor:
            return True
        bound = self._latest_strict_capture_bound
        captured_at = self._latest_strict_capture_at
        if bound is None or captured_at is None or snapshot.cursor < bound:
            return False
        return (
            bound >= context.synchronization.frontier
            or context.reader_clock < captured_at + context.interval
        )

    @staticmethod
    def _strict_synchronized(snapshot: GroupSnapshot, synchronization: SourceProgress) -> bool:
        """Check that synchronized bounds still describe this running identity."""
        return (
            snapshot.status == 'running'
            and snapshot.source_generation == synchronization.source_generation
            and snapshot.verified_incarnation == synchronization.stream_incarnation
        )

    def _strict_authority_matches(
        self,
        snapshot: GroupSnapshot,
        authority: _StrictAuthority,
        synchronization: SourceProgress,
    ) -> bool:
        """Require unchanged durable authority and synchronized source identity."""
        return self._strict_authority(snapshot) == authority and self._strict_synchronized(
            snapshot, synchronization
        )

    @staticmethod
    def _strict_result(
        snapshot: GroupSnapshot,
        *,
        join_ready: bool,
    ) -> _StrictCatchupResult:
        """Build one complete detached-strict scheduling result."""
        return _StrictCatchupResult(
            source_generation=snapshot.source_generation,
            stream_incarnation=snapshot.verified_incarnation,
            join_ready=join_ready,
            exhausted_through=None,
            exhausted_fence=None,
            exhausted_epoch_id=None,
        )

    def _transition_strict_epoch(
        self,
        context: _StrictStepContext,
        snapshot: GroupSnapshot,
        handle: SyncSG,
    ) -> tuple[GroupSnapshot, bool]:
        """Persist eligibility, deliver one safe prefix, and checkpoint."""
        if not snapshot.epochs:
            return snapshot, False
        oldest = snapshot.epochs[0]
        if oldest.state == 'waiting' and context.reader_clock >= oldest.eligibility_point:
            snapshot = context.state.mark_epoch_eligible(
                oldest.epoch_id,
                reader_clock_checkpoint=context.reader_clock,
                expected_revision=snapshot.revision,
            )
        elif oldest.state == 'waiting':
            return self._checkpoint_strict_reader(context, snapshot), False
        return self._deliver_strict_epoch(context, snapshot, handle)

    def catch_up(
        self,
        **options: Unpack[_CatchupOptions],
    ) -> None:
        """Run one durable-state-refreshed, retention-checked catch-up pass.

        Reading is bounded by the lesser of the requested frontier and the
        synchronized source frontier. Replay starts at `replay_from`
        independently of the ordinary starting cursor. `deliver_page` receives
        eligible events from at most one provider page while its concurrency
        permit is held; delivery stops before an unverified gap. The callback
        must raise if it stops before consuming the supplied events.

        Args:
            options: Cursor, frontier, delivery floor, synchronization and
                delivery callbacks, plus optional replay requirements. Optional
                `service` runs on the reader owner thread after provider calls
                with zero seconds, or owns a bounded gap wait for the supplied
                seconds. It must service due work during waits and raise on
                cooperative stop. Without it, gap waits use the reader waiter.
                Optional `between_pages` runs before each page query, after the
                previous page has been delivered and released.

        Raises:
            HistoryTruncatedError: Required history is no longer visible.
            InterruptedError: An interrupt stopped a required gap recheck.
            ValueError: Replay begins above the captured frontier or provider
                event data is invalid.
            Exception: A supplied provider, synchronization, delivery, or
                service operation fails; its failure propagates unchanged.
        """
        cursor = options['cursor']
        frontier = options['frontier']
        replay_from = options.get('replay_from')
        if replay_from is not None and replay_from > frontier:
            raise ValueError('replay start is above the source frontier')
        synchronization = options['source_progress']()
        target = min(frontier, synchronization.frontier)
        handle = self._require_open()
        floor = self._lowest_visible_id(handle)
        self._service_catchup(options)
        required = self._oldest_required(
            cursor=cursor,
            delivery_floor=options['delivery_floor'],
            target=target,
            unresolved_bounds=options.get('unresolved_bounds', ()),
            replay_from=replay_from,
        )
        self._check_floor(floor, required, target)
        if replay_from is None:
            options['cursor'] = max(cursor, min(options['delivery_floor'], target))
        if replay_from is None and options['cursor'] >= target:
            return
        self._read_and_deliver(
            handle,
            target,
            options,
        )

    def close(self) -> None:
        """Close the independent provider handle idempotently.

        Raises:
            Exception: Closing the supplied provider handle fails.
        """
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def fetch_id_runs(self, runs: Sequence[IdRun]) -> Generator[_IdRunPage]:
        """Read exact missing ID runs in ascending bounded pages.

        Args:
            runs: Ascending, disjoint inclusive ID runs.

        Yields:
            One page at a time. A full page covers through its last row;
            a short page covers through the highest ID requested by that call.

        Raises:
            RuntimeError: The reader is not open.
            ValueError: Provider events are malformed, have invalid IDs, fall
                outside the requested runs, or do not strictly ascend.
            Exception: A supplied provider query or provider-watch context
                fails.
        """
        handle = self._require_open()
        remaining = tuple(runs)
        while remaining:
            requested = remaining[:_MAX_RUNS_PER_QUERY]
            with self._query():
                rows = handle.find(
                    'EventLogEntry',
                    _id_runs_filter(requested),
                    list(EVENT_FIELDS),
                    limit=self._batch_size,
                    order=[{'field_name': 'id', 'direction': 'asc'}],
                    page=0,
                )
            events = self._events(rows)
            if any(
                not any(low <= cast('int', event['id']) <= high for low, high in requested)
                for event in events
            ):
                raise ValueError('catch-up provider page is outside the requested runs')
            covered_through = (
                cast('int', events[-1]['id'])
                if len(events) == self._batch_size
                else requested[-1][1]
            )
            yield _IdRunPage(events=events, covered_through=covered_through)
            # Release this page before reading the next one.
            del events, rows
            remaining = clip_runs(remaining, low=covered_through + 1)

    def latest_visible_id(self) -> int | None:
        """Query one fresh latest-visible event id.

        Returns:
            Latest visible positive id, or `None` for an empty source.

        Raises:
            RuntimeError: The reader is not open or its supplied monotonic
                clock moves backward.
            ValueError: The provider's visible event ID is not a positive
                integer.
            Exception: The supplied provider query or provider-watch context
                fails.
        """
        return self._latest_visible_id(self._require_open())

    def retention_floor(self) -> int | None:
        """Query the current lowest visible event id.

        Returns:
            Lowest visible positive id, or `None` when no rows remain visible.

        Raises:
            ValueError: The provider's visible event ID is not a positive
                integer.
            Exception: The supplied provider query or provider-watch context
                fails.
            RuntimeError: The reader is not open or its supplied monotonic
                clock moves backward.
        """
        return self._lowest_visible_id(self._require_open())

    def strict_step(
        self,
        **options: Unpack[_StrictCatchupOptions],
    ) -> _StrictCatchupResult:
        """Run one bounded durable detached-strict epoch scheduling step.

        The delivery callback must durably commit an event before returning
        `accepted=True`. An accepted result remains committed when
        `continue_epoch=False` stops the current epoch step.

        Args:
            options: Group authority, durable source progress, provider
                delivery, holdback policy, and optional fixed join boundary.
                Optional `service(seconds)` runs on the owner thread with zero
                seconds after provider calls and committed delivery prefixes;
                durable authority is recaptured after it returns. It also owns
                a bounded gap wait for the supplied nonzero seconds. Without
                it, gap waits use the reader waiter. The callback must
                propagate cooperative stop and owner failures.

        Returns:
            Source generation, stream incarnation, and join readiness, plus the
            exhausted-through ID, strict obligation fence, and epoch identity
            when this step proves a prefix exhausted; exhaustion fields are
            `None` otherwise.

        Raises:
            HistoryTruncatedError: Required epoch history is no longer visible.
            ValueError: Strict policy, synchronized bounds, provider event
                data, or the delivery callback's consumed-prefix result is
                invalid.
            InterruptedError: An interrupt stops a required gap recheck.
            RevisionConflictError: Durable group state changes while the group
                remains running.
            Exception: A supplied provider, state, delivery, synchronization,
                or service operation fails; its failure propagates unchanged.
        """
        holdback_secs = options['holdback_secs']
        max_pending_epochs = options['max_pending_epochs']
        if not isfinite(holdback_secs) or holdback_secs <= 0:
            raise ValueError('strict holdback must be finite and positive')
        if type(max_pending_epochs) is not int or max_pending_epochs < MIN_PENDING_EPOCHS:
            raise ValueError('max_pending_epochs must be at least two')
        state = options['state']
        snapshot = state.snapshot()
        authority = self._strict_authority(snapshot)
        self._successful_clock.resume_from(snapshot.reader_clock_checkpoint)
        synchronization = options['source_progress']()
        snapshot = self._service_strict(options)
        if not self._strict_authority_matches(snapshot, authority, synchronization):
            return self._strict_result(
                snapshot,
                join_ready=False,
            )
        handle = self._require_open()
        floor = self._lowest_visible_id(handle)
        snapshot = self._service_strict(options)
        if not self._strict_authority_matches(snapshot, authority, synchronization):
            return self._strict_result(snapshot, join_ready=False)
        target = synchronization.frontier
        boundary = options.get('join_boundary')
        if boundary is not None:
            if type(boundary) is not int or boundary < snapshot.cursor:
                raise ValueError('strict join boundary conflicts with durable progress')
            target = min(target, boundary)
        required = snapshot.cursor + 1 if snapshot.cursor < target else None
        self._check_floor(floor, required, target)
        self._latest_visible_id(handle)
        snapshot = self._service_strict(options)
        if not self._strict_authority_matches(snapshot, authority, synchronization):
            return self._strict_result(snapshot, join_ready=False)
        reader_clock = self._successful_clock.value
        interval = strict_capture_interval(holdback_secs, max_pending_epochs)
        context = _StrictStepContext(
            deliver=options['deliver'],
            holdback_secs=holdback_secs,
            interval=interval,
            join_boundary=boundary,
            max_pending_epochs=max_pending_epochs,
            reader_clock=reader_clock,
            state=state,
            synchronization=synchronization,
            target=target,
            unresolved_bounds=(
                *options.get('unresolved_bounds', ()),
                *state.unresolved_obligation_bounds(),
            ),
            obligation_fence=state.strict_obligation_fence(),
            service=options.get('service'),
        )
        try:
            snapshot = self._capture_strict_epoch(context, snapshot)
            epoch = snapshot.epochs[0] if snapshot.epochs else None
            snapshot, exhausted = self._transition_strict_epoch(context, snapshot, handle)
        except RevisionConflictError:
            current = state.snapshot()
            if current.status == 'running':
                raise
            return self._strict_result(
                current,
                join_ready=False,
            )
        join_ready = self._strict_join_ready(context, snapshot)
        result = self._strict_result(
            snapshot,
            join_ready=join_ready,
        )
        return replace(
            result,
            exhausted_through=(
                self._strict_delivery_ceiling(context, epoch)
                if exhausted and epoch is not None
                else None
            ),
            exhausted_fence=context.obligation_fence if exhausted else None,
            exhausted_epoch_id=epoch.epoch_id if exhausted and epoch is not None else None,
        )


class _SourceFindOptions(TypedDict, total=False):
    """Type the source wrapper's complete synchronous query keyword surface."""

    limit: int
    order: list[dict[str, str]] | None
    page: int


class SourceOwnerError(Exception):
    """Keep fatal source-owner failures out of the provider retry boundary.

    Attributes:
        error: Original failure to re-raise at the owner entry point.
    """

    def __init__(self, error: BaseException) -> None:
        """Retain the original fatal error for the source owner."""
        self.error = error
        super().__init__(str(error))


class _ServicingSyncSG:
    """Keep source-owned sockets moving while one serialized provider call runs."""

    def __init__(self, inner: SyncSG, service: Callable[[], None]) -> None:
        """Own one provider handle and a serial query helper."""
        self._inner = inner
        self._service = service
        self._lock = Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix=f'{current_thread().name}:query'
        )
        self._closed = False

    def close(self) -> None:
        """Close the provider after its last query and join the helper thread.

        Raises:
            Exception: Closing the supplied provider handle fails; the query
                executor is still shut down.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._inner.close()
            finally:
                self._executor.shutdown(wait=True)

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **kwargs: Unpack[_SourceFindOptions],
    ) -> list[dict[str, object]]:
        """Run one query while servicing the caller's sockets until completion.

        Provider failures retain their identity. A socket-service failure takes
        precedence, but is raised only after the provider operation finishes.

        Args:
            entity_type: Provider entity type to query.
            filters: Provider filter expression, forwarded unchanged.
            fields: Requested provider fields, forwarded unchanged.
            **kwargs: Provider limit, ordering, and page options.

        Returns:
            The supplied provider's query result.

        Raises:
            RuntimeError: The wrapper is closed or its query helper cannot
                start.
            SourceOwnerError: Servicing the source owner fails while the query
                runs; the original failure is retained in the wrapper.
            Exception: The supplied provider query fails.
        """
        with self._lock:
            if self._closed:
                raise RuntimeError('source provider handle is closed')
            completed = ThreadEvent()
            future = self._executor.submit(
                copy_context().run,
                self._inner.find,
                entity_type,
                filters,
                fields,
                **kwargs,
            )
            future.add_done_callback(lambda _future: completed.set())
            try:
                while not completed.wait(0.01):
                    self._service()
            except BaseException as error:
                completed.wait()
                raise SourceOwnerError(error) from error
            return future.result()

    def set_session_uuid(self, session_uuid: str | None) -> None:
        """Set the provider session UUID between serialized queries."""
        with self._lock:
            self._inner.set_session_uuid(session_uuid)


class _SourcePollerOptions(TypedDict):
    """Type one complete source-poller construction request."""

    state: SourceState
    fanout: EventFanout
    provider: Provider
    site_url: str
    credential: CredentialConfig
    config: SourceConfig
    max_ipc_frame_bytes: int
    source_generation: str
    waiter: _Waiter
    random_value: Callable[[], float]
    watchdog: ComponentWatchdog
    internal_deadline_secs: float
    wait_tolerance_secs: float
    health_dirty: NotRequired[Callable[[], None]]
    service: NotRequired[Callable[[], None]]
    query_service: NotRequired[Callable[[], None] | None]
    worker_instance: NotRequired[str]
    strict_retention_health: NotRequired[Callable[[], tuple[tuple[str, int, int, int], ...]]]
    successful_clock: NotRequired[_SuccessfulQueryClock | None]
    strict_buffer: NotRequired[StrictBuffer | None]


class SourcePoller:
    """Own provider and encoder resources for one shared source poller.

    The poller borrows its durable `SourceState`. It owns the provider handle
    and encoder from context entry until `close()` or context exit, and polling
    operations require that open lifetime.
    """

    def __init__(
        self,
        **options: Unpack[_SourcePollerOptions],
    ) -> None:
        """Configure one bounded source poller without opening a handle.

        Args:
            options: Durable authority, provider, policy, fanout, and injected
                timing dependencies.

        Raises:
            sqlite3.Error: Reading durable source state fails.
            RuntimeError: Source metadata is missing.
        """
        self._blocked_id: int | None = None
        self._pause_warned = False
        self._paused_since = time.monotonic() if options['state'].capacity_paused else None
        self._config = options['config']
        self._credential = options['credential']
        self._encoder: EventEncoder | None = None
        self._fanout = options['fanout']
        self._handle: SyncSG | None = None
        self._health_dirty = options.get('health_dirty', lambda: None)
        self._last_error: str | None = None
        self._last_random_sample: float | None = None
        self._local_status: SourceRunStatus | None = None
        self._max_ipc_frame_bytes = options['max_ipc_frame_bytes']
        self._internal_deadline_secs = options['internal_deadline_secs']
        self._next_retry_secs: float | None = None
        self._provider = options['provider']
        self._random_value = options['random_value']
        self._recheck_position = 0
        self._last_missing_recheck_at: float | None = None
        self._retry_attempt = 0
        self._site_url = options['site_url']
        self._service = options.get('service', lambda: None)
        self._query_service = options.get('query_service')
        self._source_generation = options['source_generation']
        self._state = options['state']
        self._stop_signaled = False
        self._poller_stopped = False
        self._strict_buffer = options.get('strict_buffer')
        self._strict_retention_health = options.get(
            'strict_retention_health',
            lambda: (),
        )
        successful_clock = options.get('successful_clock')
        self._successful_clock = (
            _SuccessfulQueryClock(initial_value=self._state.successful_clock())
            if successful_clock is None
            else successful_clock
        )
        self._waiter = options['waiter']
        self._wait_tolerance_secs = options['wait_tolerance_secs']
        self._watchdog = options['watchdog']
        self._worker_instance = options.get('worker_instance', '')

    def __enter__(self) -> Self:
        """Open the provider handle and dedicated encoder.

        Returns:
            This live source poller.

        Raises:
            Exception: Creating or configuring the supplied provider handle
                fails.
        """
        if self._handle is not None:
            return self
        with self._watchdog.watch(
            'provider-query',
            deadline_at=time.monotonic() + self._internal_deadline_secs,
        ):
            self._handle = self._provider.create_sync(
                self._site_url,
                self._credential,
            )
        if self._query_service is not None:
            self._handle = _ServicingSyncSG(self._handle, self._query_service)
        if self._paused_since is not None:
            self._paused_since = time.monotonic()
        self._encoder = EventEncoder(
            max_event_bytes=self._config.max_event_bytes,
            max_ipc_frame_bytes=self._max_ipc_frame_bytes,
        )
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Close the source poller at context exit.

        Raises:
            Exception: Closing the supplied provider handle fails.
        """
        self.close()

    def _clear_retry_diagnostics(self) -> None:
        """Clear retry detail and make owner health immediately refreshable."""
        changed = (
            self._retry_attempt != 0
            or self._next_retry_secs is not None
            or self._last_error is not None
        )
        if self._last_error is not None:
            _LOGGER.info('source %s at %s polling recovered', self._config.name, self._site_url)
        self._retry_attempt = 0
        self._next_retry_secs = None
        self._last_error = None
        if changed:
            self._health_dirty()

    def _encoded_chunks(
        self,
        encoder: EventEncoder,
        rows: Sequence[Mapping[str, object]],
    ) -> Iterator[tuple[EncodedEvent, ...]]:
        """Yield encoded chunks incrementally within count and byte bounds."""
        current: list[EncodedEvent] = []
        current_bytes = 0
        for row in rows:
            if len(current) >= self._config.batch_size:
                yield tuple(current)
                current = []
                current_bytes = 0
            encoded = encoder.encode(Event(row))
            if encoded.byte_count > self._config.commit_batch_max_bytes:
                raise SourceHaltedError(
                    f'commit-batch-capacity-exhausted: event {encoded.event_id} uses '
                    f'{encoded.byte_count} complete frame bytes; source commit_batch_max_bytes '
                    f'is {self._config.commit_batch_max_bytes}',
                    code='commit-batch-capacity-exhausted',
                )
            if (
                current
                and current_bytes + encoded.byte_count > self._config.commit_batch_max_bytes
            ):
                yield tuple(current)
                current = []
                current_bytes = 0
            current.append(encoded)
            current_bytes += encoded.byte_count
        if current:
            yield tuple(current)

    def _enter_internal(self) -> None:
        """Publish one bounded source-owned internal operation."""
        self._watchdog.transition(
            'internal',
            deadline_at=time.monotonic() + self._internal_deadline_secs,
        )

    def _enter_wait(self, delay: float) -> None:
        """Publish one wait bounded by its intended wake and tolerance."""
        self._watchdog.transition(
            'wait',
            deadline_at=time.monotonic() + delay + self._wait_tolerance_secs,
        )

    def _halt(self, code: SourceHaltStatus, error: BaseException) -> SourceHaltedError:
        """Record and construct one terminal source halt."""
        self._local_status = code
        self._last_error = str(error)
        _LOGGER.warning(
            'source %s at %s halted (%s): %s', self._config.name, self._site_url, code, error
        )
        return SourceHaltedError(f'{code}: {error}', code=code)

    def _classify_terminal_failure(
        self,
        error: EncoderHaltedError | SourceHaltedError,
    ) -> SourceHaltedError:
        """Publish one encoder or non-pause source failure as a terminal halt."""
        code = _source_halt_code(error)
        if isinstance(error, SourceHaltedError) and self._local_status == code:
            return error
        return self._halt(code, error)

    def _log_poll_failure(self, error: Exception, delay: float, *, first_failure: bool) -> None:
        """Report the first failed poll in a retry streak."""
        if first_failure:
            _LOGGER.warning(
                'source %s at %s poll failed; retrying in %ss: %s',
                self._config.name,
                self._site_url,
                delay,
                error,
            )

    def _provider_retry_delay(self) -> float:
        """Advance provider backoff and apply its bounded jitter."""
        self._retry_attempt += 1
        unjittered = saturating_retry_delay(
            self._retry_attempt,
            base_secs=self._config.poll_retry_base_secs,
            max_secs=self._config.poll_retry_max_secs,
            multiplier=self._config.poll_retry_multiplier,
        )
        random_sample = self._last_random_sample
        if random_sample is None:
            random_sample = self._random_value()
        return jitter_retry_delay(
            unjittered,
            fraction=self._config.poll_retry_jitter_fraction,
            sample=random_sample,
        )

    def _publish_committed(
        self,
        events: Sequence[EncodedEvent],
        successful_clock: float,
        *,
        late: bool = False,
    ) -> None:
        """Publish one committed chunk through eager and strict boundaries."""
        strict_buffer = self._strict_buffer
        if strict_buffer is not None:
            strict_ranges = self._state.strict_ranges_if_changed()
            if strict_ranges is not None:
                strict_buffer.synchronize_ranges(strict_ranges)
                for group, generation in self._state.replay_required_groups():
                    strict_buffer.halt_for_replay(group, generation)
            if not late:
                strict_buffer.observe_many(events, successful_time=successful_clock)
        statuses = self._fanout.publish_events(events)
        if strict_buffer is not None:
            strict_buffer.pump_many(
                tuple(status.group for status in statuses if status.ordering == 'strict'),
                successful_clock,
            )

    def _publish_range_commit(
        self,
        committed: SourceCommitResult,
        events: Sequence[EncodedEvent],
        successful_clock: float,
        *,
        late: bool = False,
    ) -> None:
        """Publish one committed range change hint and its normal delivery."""
        if committed.closed is not None:
            closed = committed.closed
            extra = self._state.log_identity()
            if closed.missing:
                log_ids_given_up(extra, closed.missing, closed.secs)
            else:
                log_gap_filled(extra, closed.low_id, closed.high_id, closed.secs)
        if committed.range_changed:
            self._fanout.publish_range_changed()
        self._publish_committed(
            events,
            successful_clock,
            late=late,
        )

    def _publish_observation_chunks(
        self,
        encoder: EventEncoder,
        rows: Sequence[Mapping[str, object]],
        successful_clock: float,
    ) -> tuple[int, SourcePause | None]:
        """Publish only admitted prefixes and stop encoding after a refused gap."""
        committed_count = 0
        for chunk in self._encoded_chunks(encoder, rows):
            committed = self._state.commit_observations(
                tuple(item.event_id for item in chunk),
                successful_clock=successful_clock,
            )
            if committed.committed_count or committed.range_changed:
                self._publish_range_commit(
                    committed, chunk[: committed.committed_count], successful_clock
                )
                committed_count += committed.committed_count
            if committed.pause is not None:
                return committed_count, committed.pause
            del chunk
        return committed_count, None

    @contextmanager
    def _query(self) -> Generator[None]:
        """Record one provider operation's clock outcome at its owner's boundary."""
        try:
            yield
        except BaseException:
            self._enter_internal()
            self._successful_clock.failed()
            raise
        self._enter_internal()
        self._successful_clock.succeeded()

    def _query_latest(self, handle: SyncSG) -> int | None:
        """Return the latest visible source id for first initialization."""
        with self._watchdog.watch(
            'provider-query',
            deadline_at=time.monotonic() + self._internal_deadline_secs,
        ):
            return edge_visible_id(handle, 'desc')

    def _query_main(self, handle: SyncSG, frontier: int) -> list[dict[str, object]]:
        """Fetch one provider batch strictly above the durable frontier."""
        with self._watchdog.watch(
            'provider-query',
            deadline_at=time.monotonic() + self._internal_deadline_secs,
        ):
            return handle.find(
                'EventLogEntry',
                [['id', 'greater_than', frontier]],
                list(EVENT_FIELDS),
                limit=self._config.batch_size,
                order=[{'field_name': 'id', 'direction': 'asc'}],
                page=0,
            )

    def _require_open(self) -> tuple[SyncSG, EventEncoder]:
        """Return live poller resources or reject use outside its context."""
        if self._handle is None or self._encoder is None:
            raise RuntimeError('source poller is not open')
        return self._handle, self._encoder

    def _require_queryable(self) -> None:
        """Reject retained data halts or a changed identity before SG queries."""
        status = self._local_status
        if status in HALTS_IN_PLACE and status != 'stream-identity-changed':
            raise SourceHaltedError(
                f'{status}: {self._last_error}', code=cast('SourceHaltStatus', status)
            )
        if not self._state.can_query:
            raise self._halt(
                'stream-identity-changed',
                SourceIdentityError('configured stream identity conflicts'),
            )

    def _serve_before_query(self) -> bool:
        """Serve queued callers without retrying source-owner failures."""
        if self._poller_stopped:
            return False
        try:
            with self._watchdog.watch(
                'internal', deadline_at=time.monotonic() + self._internal_deadline_secs
            ):
                self._service()
        except Exception as error:
            raise SourceOwnerError(error) from error
        return not self._poller_stopped

    def _perform_recheck(self, obligation_id: str) -> int:
        """Execute one unclassified bounded late-range recheck cycle."""
        handle, encoder = self._require_open()
        self._enter_internal()
        self._require_queryable()
        retained = self._state.range_for(obligation_id)
        if retained is None or retained.state == 'closed':
            return 0
        scan = self._state.begin_range_scan(
            obligation_id,
            successful_clock=self._state.successful_clock(),
        )
        processed = 0
        while True:
            if not self._serve_before_query():
                return processed
            with (
                self._query(),
                self._watchdog.watch(
                    'provider-query',
                    deadline_at=time.monotonic() + self._internal_deadline_secs,
                ),
            ):
                rows = handle.find(
                    'EventLogEntry',
                    [
                        ['id', 'greater_than', scan.scan_next_id - 1],
                        ['id', 'less_than', scan.high_id + 1],
                    ],
                    list(EVENT_FIELDS),
                    limit=self._config.batch_size,
                    order=[{'field_name': 'id', 'direction': 'asc'}],
                    page=1,
                )
            successful_clock = max(
                self._successful_clock.value,
                self._state.successful_clock(),
            )
            if not rows:
                committed = self._state.record_range_scan_page(
                    obligation_id,
                    scanned_through=scan.high_id,
                    event_ids=(),
                    successful_clock=successful_clock,
                    query_page_complete=True,
                )
                self._publish_range_commit(committed, (), successful_clock, late=True)
                return processed
            processed += len(rows)
            chunks = self._encoded_chunks(encoder, rows)
            encoded_count = 0
            while True:
                try:
                    chunk = next(chunks)
                except StopIteration:
                    break
                encoded_count += len(chunk)
                query_page_complete = encoded_count == len(rows)
                committed = self._state.record_range_scan_page(
                    obligation_id,
                    scanned_through=chunk[-1].event_id,
                    event_ids=tuple(item.event_id for item in chunk),
                    successful_clock=successful_clock,
                    query_page_complete=query_page_complete,
                )
                self._publish_range_commit(committed, chunk, successful_clock, late=True)
                if committed.closed is not None:
                    return processed
                del chunk
            scan = self._state.range_for(obligation_id)
            if scan is None:
                raise RuntimeError('range disappeared while recheck was in progress')
            if (
                scan.state == 'closed'
                or scan.scan_next_id > scan.high_id
                or scan.scan_cycle_pages >= self._config.max_range_query_pages
            ):
                return processed

    def _recheck_missing(self) -> None:
        """Walk each missing run at most once without starving higher gaps."""
        handle, encoder = self._require_open()
        self._enter_internal()
        self._require_queryable()
        ranges = tuple(item for item in self._state.ranges() if item.state == 'open')
        runs = tuple(
            run for item in ranges for run in self._state.missing_runs(item.obligation_id)
        )
        # Keep the two ascending segments separate so one cycle cannot wrap twice.
        segments = (
            clip_runs(runs, low=self._recheck_position + 1),
            clip_runs(runs, high=self._recheck_position),
        )
        pages = 0
        for segment in segments:
            remaining = segment
            while remaining and pages < self._config.max_range_query_pages:
                if not self._serve_before_query():
                    return
                requested = remaining[:_MAX_RUNS_PER_QUERY]
                with (
                    self._query(),
                    self._watchdog.watch(
                        'provider-query',
                        deadline_at=time.monotonic() + self._internal_deadline_secs,
                    ),
                ):
                    rows = handle.find(
                        'EventLogEntry',
                        _id_runs_filter(requested),
                        list(EVENT_FIELDS),
                        limit=self._config.batch_size,
                        order=[{'field_name': 'id', 'direction': 'asc'}],
                        page=0,
                    )
                pages += 1
                successful_clock = max(
                    self._successful_clock.value, self._state.successful_clock()
                )
                for retained in ranges:
                    found = [
                        row
                        for row in rows
                        if retained.low_id <= cast('int', row['id']) <= retained.high_id
                    ]
                    for chunk in self._encoded_chunks(encoder, found):
                        committed = self._state.record_missing_rows(
                            retained.obligation_id,
                            tuple(item.event_id for item in chunk),
                            successful_clock=successful_clock,
                        )
                        self._publish_range_commit(committed, chunk, successful_clock, late=True)
                covered = (
                    cast('int', rows[-1]['id'])
                    if len(rows) == self._config.batch_size
                    else requested[-1][1]
                )
                self._recheck_position = covered
                remaining = clip_runs(remaining, low=covered + 1)

    def _recheck_missing_once(self) -> None:
        """Classify terminal failures while retaining ordinary retry behavior."""
        try:
            self._recheck_missing()
        except (EncoderHaltedError, SourceHaltedError) as exc:
            halted = self._classify_terminal_failure(exc)
            if halted is exc:
                raise
            raise halted from exc

    def _recheck_missing_if_due(self) -> None:
        """Limit missing-ID rechecks to the configured fetch interval."""
        now = time.monotonic()
        if (
            self._last_missing_recheck_at is None
            or now - self._last_missing_recheck_at >= self._config.fetch_interval_secs
        ):
            self._last_missing_recheck_at = now
            self._recheck_missing_once()

    def _recheck_once(self, obligation_id: str) -> int:
        """Execute one bounded inclusive late-range recheck cycle."""
        try:
            return self._perform_recheck(obligation_id)
        except (EncoderHaltedError, SourceHaltedError) as exc:
            halted = self._classify_terminal_failure(exc)
            if halted is exc:
                raise
            raise halted from exc

    def _stamp_generation(self) -> None:
        """Keep generation-stamp failures fatal except for acquisition expiry."""
        try:
            self._state.stamp_generation()
        except WriteLockTimeoutError:
            raise
        except Exception as error:
            raise SourceOwnerError(error) from error

    def _poll_once(self) -> int:
        """Execute one initialization or main-stream polling attempt."""
        handle, encoder = self._require_open()
        self._enter_internal()
        if not self._state.stamped:
            self._stamp_generation()
        self._require_queryable()
        self._last_random_sample = self._random_value()
        current = self._state.begin_cycle()
        if not current.initialized:
            with self._query():
                latest_id = self._query_latest(handle)
            self._state.initialize(
                latest_id,
                incarnation=self._source_generation,
            )
            self._clear_retry_diagnostics()
            return 0
        with self._query():
            rows = self._query_main(handle, current.frontier)
        successful_clock = self._successful_clock.value
        try:
            committed_count, pause = self._publish_observation_chunks(
                encoder,
                rows,
                successful_clock,
            )
            if committed_count == 0:
                self._state.record_successful_query(successful_clock)
                self._publish_committed((), successful_clock)
        except (EncoderHaltedError, SourceHaltedError) as exc:
            raise self._classify_terminal_failure(exc) from exc
        self._local_status = None
        self._clear_retry_diagnostics()
        if pause is not None:
            self._blocked_id = max(self._blocked_id or 0, pause.blocked_id)
            if self._paused_since is None:
                self._paused_since = time.monotonic()
            if not self._pause_warned:
                log_paused(
                    self._state.log_identity(),
                    pause.setting,
                    pause.limit,
                    pause.count,
                )
                self._pause_warned = True
            return 0
        if (
            committed_count
            and self._paused_since is not None
            and (
                self._blocked_id is None
                or max(current.frontier, cast('int', rows[-1]['id'])) >= self._blocked_id
            )
        ):
            self._state.end_capacity_pause()
            log_resumed(
                self._state.log_identity(),
                time.monotonic() - self._paused_since,
            )
            self._blocked_id = None
            self._paused_since = None
            self._pause_warned = False
        return len(rows)

    def adopt_stream(
        self,
        *,
        adoption_id: str,
        command_fingerprint: str,
        new_stream_fingerprint: str,
        reason: str,
        groups_parked: bool,
    ) -> SourceSnapshot:
        """Adopt, initialize, and resume one supervisor-fenced stream.

        Durable adoption commits before the latest-row provider query. If the
        query fails after a fresh adoption or while its current state remains
        uninitialized, the replacement stream remains adopted but
        uninitialized. Retrying the exact command preserves its current state
        and retries initialization; an already initialized retry remains
        initialized if that query fails.

        Args:
            adoption_id: Durable per-halt adoption reservation.
            command_fingerprint: Stable semantic operator command identity.
            new_stream_fingerprint: Configured replacement stream identity.
            reason: Required operator audit reason.
            groups_parked: Supervisor evidence that dependent groups are
                fenced.

        Returns:
            FULL-initialized replacement-stream snapshot.

        Raises:
            SourceIdentityError: Adoption fencing, reservation, stream or
                command identity, or audit reason conflicts with durable source
                state.
            ValueError: The provider's latest visible event ID is invalid.
            Exception: A provider query or durable-state operation fails. Fresh
                adoption can already be committed when a later query or
                initialization fails.
        """
        handle, _encoder = self._require_open()
        self._enter_internal()
        self._state.adopt_stream(
            adoption_id=adoption_id,
            command_fingerprint=command_fingerprint,
            new_stream_fingerprint=new_stream_fingerprint,
            reason=reason,
            groups_parked=groups_parked,
        )
        with self._query():
            latest_id = self._query_latest(handle)
        snapshot = self._state.initialize(latest_id)
        self._local_status = None
        self._clear_retry_diagnostics()
        return snapshot

    def close(self) -> None:
        """Release the encoder and close the provider handle idempotently.

        Raises:
            Exception: Closing the supplied provider handle fails.
        """
        self._encoder = None
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def event_channel_lost(self, group: str, *, reason: str = 'event-channel-lost') -> None:
        """Detach one group and release every source-retained live body.

        Args:
            group: Stable group identity whose channel ended.
            reason: Body-free detach condition used for catch-up evidence.
        """
        self._fanout.detach(group, reason=reason)
        if self._strict_buffer is not None:
            self._strict_buffer.detach_group(group, reason=reason)

    def health_from_snapshot(self, snapshot: SourceSnapshot) -> SourceHealth:
        """Build complete source health from an existing durable snapshot.

        Args:
            snapshot: Complete immutable source-state projection.

        Returns:
            Durable and process-local source health without another snapshot
            query.

        Raises:
            sqlite3.Error: Reading audit counts fails.
            OSError: Inspecting the source database family fails.
            Exception: The supplied strict-retention health projection fails.
        """
        strict_buffer = self._strict_buffer
        return SourceHealth(
            site=snapshot.site,
            source=snapshot.source,
            source_generation=self._source_generation,
            status=snapshot.status if self._local_status is None else self._local_status,
            frontier=snapshot.frontier,
            successful_clock=snapshot.successful_clock,
            retry_attempt=self._retry_attempt,
            next_retry_secs=self._next_retry_secs,
            open_range_count=sum(item.state == 'open' for item in snapshot.ranges),
            late_id_count=sum(item.late_id_count for item in snapshot.ranges),
            last_error=self._last_error,
            worker_instance=self._worker_instance,
            initialized=snapshot.initialized if self._worker_instance else False,
            pending_obligation_count=snapshot.pending_obligations,
            orphan_marker_count=len(snapshot.replay_markers),
            strict_buffer_event_count=0 if strict_buffer is None else strict_buffer.event_count,
            strict_buffer_byte_count=0 if strict_buffer is None else strict_buffer.byte_count,
            strict_retention=self._strict_retention_health(),
            stream_incarnation=snapshot.stream_incarnation,
            audit_count=self._state.audit_count(),
            database_size_bytes=self._state.database_size_bytes(),
        )

    @property
    def health(self) -> SourceHealth:
        """Return complete immutable source worker health.

        Returns:
            Durable source state plus process-local retry or encoder state.

        Raises:
            sqlite3.Error: Reading durable source state or audit counts fails.
            OSError: Inspecting the source database family fails.
            Exception: The supplied strict-retention health projection fails.
            RuntimeError: Source metadata is missing or its persisted condition
                is unknown.
        """
        return self.health_from_snapshot(self._state.snapshot())

    def run(self, *, max_cycles: int | None = None) -> None:
        """Run polls, interval-limited missing-ID rechecks, and due scans.

        Ordinary polling failures retry with configured backoff and jitter.
        Lock expiry waits one base interval without growing provider backoff.
        Terminal source halts, generation-stamp failures other than lock
        expiry, and cooperative write stops propagate.

        Args:
            max_cycles: Optional deterministic attempt bound.

        Raises:
            SourceHaltedError: An encoder, event/frame size, or stream-identity
                failure prevents polling.
            WriteLockStoppedError: Source write acquisition was stopped
                cooperatively.
            Exception: Generation stamping or a source-owner service operation
                fails;
                the original owner failure propagates instead of entering
                provider retry.
        """
        cycles = 0
        while not self._poller_stopped and (max_cycles is None or cycles < max_cycles):
            cycles += 1
            cycle_retry_attempt = self._retry_attempt
            try:
                count = self._poll_once()
                self._retry_attempt = cycle_retry_attempt
                self._recheck_missing_if_due()
                due = self._state.due_range_id()
                if due is not None and not self._poller_stopped:
                    self._recheck_once(due)
            except SourceHaltedError:
                raise
            except BaseException as exc:
                _raise_source_owner_error(exc)
                if not isinstance(exc, Exception):
                    raise
                raise_owner_write_failure(exc)
                if isinstance(exc, WriteLockTimeoutError):
                    self._retry_attempt = cycle_retry_attempt
                    delay = self._config.poll_retry_base_secs
                else:
                    delay = self._provider_retry_delay()
                self._log_poll_failure(exc, delay, first_failure=cycle_retry_attempt == 0)
                self._next_retry_secs = delay
                self._last_error = str(exc)
                self._health_dirty()
                self._enter_wait(delay)
                if self._waiter.wait(delay):
                    self._poller_stopped = True
                continue
            self._clear_retry_diagnostics()
            if count >= self._config.batch_size:
                continue
            self._enter_wait(self._config.fetch_interval_secs)
            if self._waiter.wait(self._config.fetch_interval_secs):
                self._poller_stopped = True

    def stop(self) -> None:
        """Interrupt every current or future poller wait idempotently."""
        self._poller_stopped = True
        if not self._stop_signaled:
            self._waiter.set()
            self._stop_signaled = True


def _id_runs_filter(runs: Sequence[IdRun]) -> list[object]:
    """Match any of the inclusive ID runs in one provider filter."""
    return [
        {'filter_operator': 'any', 'filters': [['id', 'between', low, high] for low, high in runs]}
    ]


def _raise_source_owner_error(error: BaseException) -> None:
    """Unwrap fatal owner failures without replacing their original cause."""
    if isinstance(error, SourceOwnerError):
        raise error.error from error.error.__cause__


def _source_halt_code(error: BaseException) -> SourceHaltStatus:
    """Return the closed source status encoded by one raised error."""
    code = error.code if isinstance(error, (EncoderHaltedError, SourceHaltedError)) else None
    if code is None:
        raise RuntimeError('unknown source halt code: None') from error
    try:
        return _SOURCE_HALT_CODES[code]
    except KeyError:
        raise RuntimeError(f'unknown source halt code: {code}') from error


def edge_visible_id(handle: SyncSG, direction: Literal['asc', 'desc']) -> int | None:
    """Read and validate the lowest or latest visible provider event id.

    Args:
        handle: Open synchronous provider handle.
        direction: `asc` selects the lowest id; `desc` selects the latest id.

    Returns:
        A positive event id, or `None` when no event is visible.

    Raises:
        ValueError: The visible event id is not a positive integer.
        Exception: The supplied provider query fails.
    """
    rows = handle.find(
        'EventLogEntry',
        [],
        ['id'],
        limit=1,
        order=[{'field_name': 'id', 'direction': direction}],
        page=0,
    )
    if not rows:
        return None
    value = rows[0].get('id')
    if type(value) is not int or value <= 0:
        edge = 'lowest' if direction == 'asc' else 'latest'
        raise ValueError(f'{edge} visible EventLogEntry id is invalid')
    return value


def history_truncation(
    floor: int | None, required: int | None, frontier: int
) -> HistoryTruncatedError | None:
    """Classify required history against provider visibility and durable progress.

    Args:
        floor: Lowest visible provider event id, or `None` for no visible rows.
        required: Oldest required event id, or `None` when no history is
            needed.
        frontier: Durable source frontier captured before querying the floor.

    Returns:
        Truncation evidence when the visible floor exceeds the required id, or
        when empty visible history follows positive durable progress. Empty
        history uses `frontier + 1` as its effective floor. Otherwise `None`.
    """
    if required is None:
        return None
    if floor is None and frontier > 0:
        return HistoryTruncatedError(frontier + 1, required, no_visible_rows=True)
    if floor is not None and floor > required:
        return HistoryTruncatedError(floor, required)
    return None


def jitter_retry_delay(delay: float, *, fraction: float, sample: float) -> float:
    """Apply symmetric retry jitter using a caller-owned unit-interval sample.

    Args:
        delay: Unjittered retry delay in seconds.
        fraction: Fractional width on either side of the unjittered delay.
        sample: Sample from zero through one; one half leaves the delay
            unchanged.

    Returns:
        Delay in seconds scaled between `1 - fraction` and `1 + fraction`.
    """
    return delay * (1.0 + fraction * (2.0 * sample - 1.0))


def saturating_retry_delay(
    attempt: int,
    *,
    base_secs: float,
    max_secs: float,
    multiplier: float,
) -> float:
    """Return one bounded retry delay without overflowing an intermediate.

    Args:
        attempt: Retry attempt; values at or below one use the initial delay.
        base_secs: Finite positive initial delay in seconds.
        max_secs: Finite positive upper bound in seconds.
        multiplier: Finite growth factor at least one.

    Returns:
        Initial delay clamped to the maximum, then increased by the multiplier
        for each later attempt until it saturates at the maximum.
    """
    delay = min(base_secs, max_secs)
    remaining = attempt - 1
    if remaining <= 0 or delay >= max_secs or multiplier == 1.0:
        return delay
    # The positive binary64 range spans less than a factor of 2**2098.
    # Its smallest multiplier above one crosses that span within 2**64 steps.
    if remaining.bit_length() > _MAX_RETRY_DELAY_STEP_BITS:
        return max_secs

    candidate_log2 = log2(delay) + float(remaining) * log2(multiplier)
    if candidate_log2 >= log2(max_secs):
        return max_secs
    # One arithmetic path keeps successive attempts monotone, including the
    # subnormal range. Float rounding may saturate just before the exact cap.
    return min(max_secs, max(delay, exp2(candidate_log2)))
