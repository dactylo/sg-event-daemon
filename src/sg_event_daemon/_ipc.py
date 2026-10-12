"""Closed local worker-control and event IPC protocols."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import errno
import json
import os
import selectors
import socket
import stat
import struct
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable, Collection, Iterator, Mapping
from contextlib import suppress
from dataclasses import asdict, dataclass, field, replace
from functools import partial
from math import isfinite
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Literal,
    NotRequired,
    Protocol,
    Self,
    TypeAlias,
    TypedDict,
    Unpack,
    cast,
)

from ._event_frame import (
    EVENT_SCHEMA,
    EVENT_VERSION,
    encode_event_header,
    event_frame_bytes,
)
from ._events import decode_event
from ._fanout import (
    EncodedEvent,
    EventDelivery,
    FanoutMessage,
    FanoutStatus,
    GroupChannelInterface,
    RangeChanged,
    StrictCapacityControl,
    StrictOrderingControl,
    strict_control_metadata,
)
from ._group_types import (
    AuditIdentity,
    ConditionalParkDirective,
    ConditionalParkOutcome,
    LiveJoinIdentity,
    StartMode,
    is_start_mode,
)
from ._health import (
    HEALTH_SECRET_TEXT_TERMS,
    EventSocketHealth,
    GroupHealth,
    WatchdogHealth,
    WorkerHealth,
    looks_secret,
)
from ._identity import canonical_bytes
from ._platform import (
    FILE_MODE,
    SecurityError,
    peer_identity,
    pending_socket_bytes,
    remove_stale_socket,
)
from ._resources import close_callables, close_resources
from ._source import (
    SOURCE_RUN_STATUSES,
    Attached,
    JoinAbort,
    JoinAttemptIdentity,
    JoinConfirm,
    JoinFinalize,
    JoinFinalized,
    JoinProtocolError,
    JoinRequest,
    JoinRetry,
    JoinTarget,
    SourceHealth,
)
from ._watchdog import GROUP_PHASES, SOURCE_WATCHDOG_PHASES, WATCHDOG_KINDS, WATCHDOG_PHASES
from ._write_lock import raise_owner_write_failure

if TYPE_CHECKING:
    from pathlib import Path

    from ._source import SourceRunStatus
    from ._watchdog import GroupPhase, SourceWatchdogPhase, WatchdogKind, WatchdogPhase

_JoinValue: TypeAlias = """(
    GroupBootstrapRequest | JoinAbort | JoinConfirm |
    JoinFinalize | JoinRequest |
    StrictCapacityControl | StrictOrderingControl
)"""

_AUTH_SCHEMA = 'sg-event-daemon-event-auth'


_CONTROL_KINDS = frozenset(
    {
        'assignment',
        'command-result',
        'event-peer-grant',
        'event-peer-ready',
        'group-recovery-ready',
        'health',
        'hello',
        'operator-command',
        'source-endpoint-ready',
        'stop',
    }
)
_CONTROL_PROTOCOL_VERSION = 1
_CONTROL_SCHEMA = 'sg-event-daemon-worker-control'
_EVENT_AUTH_PROTOCOL_VERSION = 1
_EVENT_KINDS = frozenset(
    {
        'group-bootstrap-observe',
        'detach',
        'event',
        'join-abort',
        'join-acknowledge',
        'join-confirm',
        'join-finalize',
        'join-reply',
        'join-request',
        'range-changed',
        'strict-capacity',
        'strict-ordering',
    }
)
_EVENT_SOCKET_UMASK_LOCK = threading.Lock()
_LENGTH_BYTES = 4
_MAX_AUTH_FRAME_BYTES = 65_536
_MAX_BOOTSTRAP_RETRY_SECS = 60.0
_PAIR_LENGTH = 2

# IPC matches affixes in structured health keys and includes capability;
# logging and control reject exact names, including auth and plural forms.
_SECRET_KEY_TERMS = frozenset(
    {'api_key', 'apikey', 'capability', 'credential', 'password', 'secret', 'token'}
)
_SECRET_VALUE_TERMS = HEALTH_SECRET_TEXT_TERMS
_STRICT_RETENTION_LENGTH = 4
_UNIX_STREAM_DISCONNECT_ERRNOS = frozenset(
    {
        errno.ECONNABORTED,
        errno.ECONNRESET,
        errno.ENOTCONN,
        errno.EPIPE,
        errno.ESHUTDOWN,
    }
)

ControlMessage: TypeAlias = """(
    ControlHello | StopCommand | HealthUpdate | OperatorCommand | CommandResult |
    EventPeerGrant | EventPeerReady | GroupRecoveryReady | SourceEndpointReady |
    WorkerAssignment
)"""
_JoinResult: TypeAlias = """(
    Attached | GroupBootstrapObservation |
    GroupBootstrapPending | JoinFinalized |
    JoinRetry | JoinTarget | None
)"""
SourceGroupAssignment: TypeAlias = (
    'ActiveSourceGroupAssignment | PendingDispositionSourceGroupAssignment'
)


@dataclass(frozen=True)
class ActiveSourceGroupAssignment:
    """Inventory one configured group for its source-owning worker."""

    source: str
    group: str
    group_generation: int
    start_mode: StartMode

    def __post_init__(self) -> None:
        """Validate one complete source-group inventory entry.

        Raises:
            ValueError: A source or group name is empty, `group_generation` is
                not an exact nonnegative integer, or `start_mode` is neither
                `initialize` nor `resume`.
        """
        if not self.source or not self.group:
            raise ValueError('source group assignment identity must not be empty')
        if type(self.group_generation) is not int or self.group_generation < 0:
            raise ValueError('source group assignment generation is invalid')
        if not is_start_mode(self.start_mode):
            raise ValueError('source group assignment start mode is invalid')


@dataclass(frozen=True)
class CommandResult:
    """Carry one frozen request-correlated operator-command result.

    `result` is copied into recursively immutable mappings and tuples after
    validating its supported secret-free values. Secret-looking `message` text
    is replaced with `<redacted>`.
    """

    command: str
    ok: bool
    message: str
    request_id: str
    result: Mapping[str, object]

    def __post_init__(self) -> None:
        """Validate fields and redact secret-looking result text.

        Raises:
            TypeError: `ok` is not an exact boolean, a result mapping has
                non-string keys, or a result value is not a supported scalar,
                mapping, list, or tuple.
            ValueError: `command` or `request_id` is empty, or the result
                contains secret-looking keys or text.
        """
        if not self.command or not self.request_id:
            raise ValueError('command result command must not be empty')
        if type(self.ok) is not bool:
            raise TypeError('command result status must be a boolean')
        if looks_secret(self.message):
            object.__setattr__(self, 'message', '<redacted>')
        object.__setattr__(
            self,
            'result',
            _freeze_health_payload(self.result),
        )


@dataclass(frozen=True)
class ControlHello:
    """Identify the first message sent by one exec-spawned worker."""

    spawn_instance: str
    worker_id: str
    pid: int

    def __post_init__(self) -> None:
        """Validate complete non-secret worker identity.

        Raises:
            ValueError: `spawn_instance` or `worker_id` is empty, or `pid` is
                not an exact positive integer.
        """
        if not self.spawn_instance or not self.worker_id:
            raise ValueError('worker hello identity must not be empty')
        if type(self.pid) is not int or self.pid <= 0:
            raise ValueError('worker hello pid must be positive')


@dataclass(frozen=True)
class DeliveryGroupAssignment:
    """Assign one worker-owned delivery lane and optional recovery."""

    source: str
    group: str
    group_generation: int
    start_mode: StartMode
    conditional_park: ConditionalParkDirective | None = None

    def __post_init__(self) -> None:
        """Validate one complete lane assignment and optional recovery fence.

        Raises:
            TypeError: `conditional_park` is neither a
                `ConditionalParkDirective` nor `None`.
            ValueError: A source or group name is empty, `group_generation` is
                not an exact nonnegative integer, `start_mode` is neither
                `initialize` nor `resume`, or the directive conflicts with the
                assigned group or generation.
        """
        if not self.source or not self.group:
            raise ValueError('group assignment identity must not be empty')
        if type(self.group_generation) is not int or self.group_generation < 0:
            raise ValueError('group assignment generation is invalid')
        if not is_start_mode(self.start_mode):
            raise ValueError('group assignment start mode is invalid')
        directive = cast('object', self.conditional_park)
        if directive is None:
            return
        if not isinstance(directive, ConditionalParkDirective):
            raise TypeError('group assignment recovery directive is invalid')
        if directive.group != self.group or directive.group_generation != self.group_generation:
            raise ValueError('group assignment recovery identity conflicts')


@dataclass(frozen=True)
class EventConnectionStatus:
    """Describe one authenticated event peer without retaining bodies.

    Attributes:
        queued_count: Number of queued event frames.
        queued_bytes: Total bytes in queued event frames.
    """

    _group: str
    _connected: bool
    queued_count: int
    queued_bytes: int
    _reason: str | None


@dataclass(frozen=True)
class EventPeerGrant:
    """Authorize one exact dependent spawn on one source/group endpoint."""

    source: str
    group: str
    dependent_spawn_instance: str
    capability: str

    def __post_init__(self) -> None:
        """Validate complete nonempty grant identity."""
        if not all(
            (
                self.source,
                self.group,
                self.dependent_spawn_instance,
                self.capability,
            )
        ):
            raise ValueError('event peer grant fields must not be empty')


@dataclass(frozen=True)
class EventPeerReady:
    """Acknowledge installation of one exact dependent event grant."""

    source: str
    group: str
    dependent_spawn_instance: str

    def __post_init__(self) -> None:
        """Validate complete nonempty acknowledgement identity."""
        if not all((self.source, self.group, self.dependent_spawn_instance)):
            raise ValueError('event peer ready fields must not be empty')


@dataclass(frozen=True)
class GroupBootstrapObservation:
    """Return live source facts captured before group state opens.

    `source_successful_clock` is the source's successful-query clock in
    seconds, not a wall-clock timestamp.
    """

    source: str
    group: str
    group_generation: int
    source_generation: str
    stream_incarnation: str
    captured_frontier: int
    source_successful_clock: float

    def __post_init__(self) -> None:
        """Validate one complete non-secret live observation.

        Raises:
            ValueError: A source, group, source-generation, or incarnation
                identity is empty; `group_generation` or `captured_frontier` is
                not an exact nonnegative integer; or `source_successful_clock`
                is neither an integer nor a float, or is less than zero.
        """
        if not all(
            (
                self.source,
                self.group,
                self.source_generation,
                self.stream_incarnation,
            )
        ):
            raise ValueError('group bootstrap observation fields must not be empty')
        if type(self.group_generation) is not int or self.group_generation < 0:
            raise ValueError('group bootstrap observation generation is invalid')
        if type(self.captured_frontier) is not int or self.captured_frontier < 0:
            raise ValueError('group bootstrap frontier is invalid')
        if (
            type(self.source_successful_clock) not in {float, int}
            or self.source_successful_clock < 0
        ):
            raise ValueError('group bootstrap successful clock is invalid')


@dataclass(frozen=True)
class GroupBootstrapPending:
    """Tell one exact group generation to retry source bootstrap later.

    The retry targets the same `source`, `group`, and `group_generation` and
    requires `0 < retry_after_secs <= 60`, in seconds.
    """

    source: str
    group: str
    group_generation: int
    retry_after_secs: float

    def __post_init__(self) -> None:
        """Validate bootstrap identity and normalize the bounded retry delay.

        Raises:
            ValueError: A source or group name is empty, `group_generation` is
                not an exact nonnegative integer, or `retry_after_secs` is not
                an integer or float satisfying `0 < value <= 60`.
        """
        if not self.source or not self.group:
            raise ValueError('group bootstrap pending identity must not be empty')
        if type(self.group_generation) is not int or self.group_generation < 0:
            raise ValueError('group bootstrap pending generation is invalid')
        delay = self.retry_after_secs
        if type(delay) not in {float, int} or not 0 < delay <= _MAX_BOOTSTRAP_RETRY_SECS:
            raise ValueError('group bootstrap pending delay is invalid')
        object.__setattr__(self, 'retry_after_secs', float(delay))


@dataclass(frozen=True)
class GroupBootstrapRequest:
    """Request one direct source observation for a configured group."""

    source: str
    group: str
    group_generation: int
    start_mode: StartMode

    def __post_init__(self) -> None:
        """Validate one exact bootstrap request.

        Raises:
            ValueError: A source or group name is empty, `group_generation` is
                not an exact nonnegative integer, or `start_mode` is neither
                `initialize` nor `resume`.
        """
        if not self.source or not self.group:
            raise ValueError('group bootstrap identity must not be empty')
        if type(self.group_generation) is not int or self.group_generation < 0:
            raise ValueError('group bootstrap generation is invalid')
        if not is_start_mode(self.start_mode):
            raise ValueError('group bootstrap start mode is invalid')


@dataclass(frozen=True)
class GroupRecoveryReady:
    """Report one exact durable group-side recovery result."""

    spawn_instance: str
    evidence_id: str
    site: str
    group: str
    group_generation: int
    outcome: ConditionalParkOutcome
    resulting_revision: int

    def __post_init__(self) -> None:
        """Validate complete non-secret recovery identity and result.

        Raises:
            ValueError: A spawn, evidence, site, or group identity is empty or
                contains secret-looking text; `group_generation` or
                `resulting_revision` is not an exact nonnegative integer; or
                `outcome` is neither `parked` nor `stale`.
        """
        if not self.spawn_instance or not self.evidence_id or not self.site or not self.group:
            raise ValueError('group recovery ready identity must not be empty')
        if type(self.group_generation) is not int or self.group_generation < 0:
            raise ValueError('group recovery ready generation is invalid')
        if self.outcome not in {'parked', 'stale'}:
            raise ValueError('group recovery ready outcome is invalid')
        if type(self.resulting_revision) is not int or self.resulting_revision < 0:
            raise ValueError('group recovery ready revision is invalid')
        if any(
            term in value.casefold()
            for value in (
                self.spawn_instance,
                self.evidence_id,
                self.site,
                self.group,
            )
            for term in _SECRET_VALUE_TERMS
        ):
            raise ValueError('group recovery ready contains secret-looking text')


@dataclass(frozen=True)
class HealthUpdate:
    """Carry one secret-free structured worker health payload."""

    worker: WorkerHealth
    watchdogs: tuple[WatchdogHealth, ...]
    sources: tuple[SourceHealth, ...]
    groups: tuple[GroupHealth, ...]
    event_sockets: tuple[EventSocketHealth, ...]

    def __post_init__(self) -> None:
        """Validate one closed typed secret-free health update.

        Raises:
            TypeError: A nested mapping has non-string keys or a value is not a
                supported scalar, mapping, list, or tuple.
            ValueError: The payload contains secret-looking keys or text.
        """
        _freeze_health_payload(cast('dict[str, object]', asdict(self)))


@dataclass(frozen=True)
class OperatorCommand:
    """Carry one frozen request-correlated operator command to its owner.

    `arguments` is copied into recursively immutable mappings and tuples after
    validating its supported secret-free values.
    """

    request_id: str
    command: str
    arguments: Mapping[str, object]
    identity: AuditIdentity

    def __post_init__(self) -> None:
        """Validate and freeze one complete body-free command.

        Raises:
            TypeError: An argument mapping has non-string keys or a value is
                not a supported scalar, mapping, list, or tuple.
            ValueError: `request_id` or `command` is empty, or arguments
                contain secret-looking keys or text.
        """
        if not self.request_id or not self.command:
            raise ValueError('operator command identity must not be empty')
        object.__setattr__(
            self,
            'arguments',
            _freeze_health_payload(self.arguments),
        )


@dataclass(frozen=True)
class PendingDispositionSourceGroupAssignment:
    """Inventory one configured group awaiting audited disposition."""

    source: str
    group: str
    orphan_generation: int
    reserved_generation: int

    def __post_init__(self) -> None:
        """Validate one exact pending source generation fence.

        Raises:
            ValueError: A source or group name is empty, `orphan_generation` is
                not an exact nonnegative integer, or `reserved_generation` is
                not an exact integer greater than `orphan_generation`.
        """
        if not self.source or not self.group:
            raise ValueError('pending source assignment identity must not be empty')
        if type(self.orphan_generation) is not int or self.orphan_generation < 0:
            raise ValueError('pending source orphan generation is invalid')
        if (
            type(self.reserved_generation) is not int
            or self.reserved_generation <= self.orphan_generation
        ):
            raise ValueError('pending source reserved generation must increase')


@dataclass(frozen=True)
class SocketBufferSizes:
    """Record requested and platform-reported socket buffer sizes in bytes.

    Attributes:
        requested: Requested send and receive buffer size in bytes.
        send: Platform-reported effective send buffer size in bytes.
        receive: Platform-reported effective receive buffer size in bytes.
    """

    requested: int
    send: int
    receive: int


@dataclass(frozen=True)
class SourceEndpointReady:
    """Report one opened source endpoint and assigned source generation."""

    source: str
    source_generation: str
    path: str

    def __post_init__(self) -> None:
        """Validate complete endpoint bootstrap data."""
        if not self.source or not self.source_generation or not self.path:
            raise ValueError('source endpoint fields must not be empty')


@dataclass(frozen=True)
class StopCommand:
    """Request graceful worker shutdown."""

    reason: str

    def __post_init__(self) -> None:
        """Validate the body-free stop reason."""
        if not self.reason:
            raise ValueError('stop reason must not be empty')


@dataclass(frozen=True)
class WorkerAssignment:
    """Carry one immutable supervisor-decided worker bootstrap assignment.

    `source_generations` contains `(source, generation)` pairs owned by this
    worker. A source-owning assignment cannot consume remote source endpoints
    or event grants, while a dependent assignment cannot own source-group
    inventory.
    """

    worker_id: str
    spawn_instance: str
    source_generations: tuple[tuple[str, str], ...]
    group_assignments: tuple[DeliveryGroupAssignment, ...]
    source_group_assignments: tuple[SourceGroupAssignment, ...]
    source_endpoints: tuple[SourceEndpointReady, ...]
    event_grants: tuple[EventPeerGrant, ...]

    def __post_init__(self) -> None:
        """Validate unique immutable assignment identities and generations.

        Raises:
            ValueError: Worker or spawn identity is empty; a source name or
                generation is empty; sources, worker-owned groups, recovery
                evidence ids, source-inventory groups, endpoint sources, or
                source/group grant pairs repeat; a source-inventory group names
                an unowned source; a grant names another spawn; or the
                source-owner and dependent-role constraints conflict.
        """
        if not self.worker_id or not self.spawn_instance:
            raise ValueError('worker assignment identity must not be empty')
        source_names = self._source_names()
        self._validate_group_assignments()
        self._validate_source_groups(source_names)
        self._validate_remote_assignments(source_names)

    def _source_names(self) -> set[str]:
        """Validate source generations and return their unique names."""
        source_names: set[str] = set()
        for source, generation in self.source_generations:
            if not source or not generation:
                raise ValueError('source assignment fields must not be empty')
            if source in source_names:
                raise ValueError('source assignment must be unique')
            source_names.add(source)
        return source_names

    def _validate_group_assignments(self) -> None:
        """Validate that every worker-owned group appears exactly once."""
        evidence_ids: set[str] = set()
        group_names: set[str] = set()
        for assignment in self.group_assignments:
            if assignment.group in group_names:
                raise ValueError('group assignment must be unique')
            group_names.add(assignment.group)
            directive = assignment.conditional_park
            if directive is None:
                continue
            if directive.evidence_id in evidence_ids:
                raise ValueError('group recovery evidence must be unique')
            evidence_ids.add(directive.evidence_id)

    def _validate_remote_assignments(self, source_names: set[str]) -> None:
        """Validate endpoint, grant, and source-owner role constraints."""
        endpoint_sources: set[str] = set()
        for endpoint in self.source_endpoints:
            if endpoint.source in endpoint_sources:
                raise ValueError('source endpoint assignment must be unique')
            endpoint_sources.add(endpoint.source)
        grant_pairs: set[tuple[str, str]] = set()
        for grant in self.event_grants:
            if grant.dependent_spawn_instance != self.spawn_instance:
                raise ValueError('event grant spawn conflicts with worker assignment')
            pair = (grant.source, grant.group)
            if pair in grant_pairs:
                raise ValueError('event grant assignment must be unique')
            grant_pairs.add(pair)
        if source_names and (self.source_endpoints or self.event_grants):
            raise ValueError('source-owning assignment cannot consume remote endpoints')
        if not source_names and self.source_group_assignments:
            raise ValueError('dependent assignment cannot own source group inventory')

    def _validate_source_groups(self, source_names: set[str]) -> None:
        """Validate complete source-owned group inventory identities."""
        source_group_names: set[str] = set()
        for assignment in self.source_group_assignments:
            if assignment.group in source_group_names:
                raise ValueError('source group assignment must be unique')
            if assignment.source not in source_names:
                raise ValueError('source group assignment source is not owned')
            source_group_names.add(assignment.group)


class WorkerControl:
    """Own one inherited same-UID duplex worker-control socket.

    Calls to `send` from concurrent writers are serialized, but the shared
    receive reader and message queue require one reader. Closing or poisoning
    the channel closes its owned transport permanently.
    """

    def __init__(
        self,
        sock: socket.socket,
        *,
        max_frame_bytes: int,
        send_deadline_secs: float,
    ) -> None:
        """Own one permanently nonblocking shared control channel.

        Args:
            sock: Connected inherited control socket.
            max_frame_bytes: Positive maximum control frame body bytes.
            send_deadline_secs: Finite positive complete-operation send
                deadline in seconds.

        Raises:
            ValueError: The socket is not Unix-domain or a bound is invalid.
        """
        if sock.family != socket.AF_UNIX:
            raise ValueError('worker control requires a Unix socket')
        if max_frame_bytes <= 0:
            raise ValueError('maximum frame bytes must be positive')
        if not isfinite(send_deadline_secs) or send_deadline_secs <= 0:
            raise ValueError('worker control send deadline must be finite and positive')
        sock.settimeout(0.0)
        self._max_frame_bytes = max_frame_bytes
        self._messages: deque[ControlMessage] = deque()
        self._reader = _RawFrameReader(max_frame_bytes=max_frame_bytes)
        self._send_lock = threading.Lock()
        self._send_deadline_secs = send_deadline_secs
        self._send_error: OSError | None = None
        self._state_lock = threading.Lock()
        self._socket: socket.socket | None = sock
        self._socket_buffers = _request_socket_buffers(sock, max_frame_bytes)

    def _open_socket(self) -> socket.socket:
        """Return the open descriptor or fail the poisoned channel."""
        with self._state_lock:
            sock = self._socket
        if sock is None:
            raise EOFError('worker control is closed')
        return sock

    def _poison(self, *, send_error: OSError | None = None) -> None:
        """Atomically make the channel unusable and close it exactly once."""
        with self._state_lock:
            sock = self._socket
            self._socket = None
            if send_error is not None and self._send_error is None:
                self._send_error = send_error
            self._messages.clear()
        if sock is not None:
            sock.close()

    def _receive_available(
        self,
        sock: socket.socket,
        queued: int,
        remaining: int,
        snapshot_error: OSError | ValueError | None,
    ) -> Iterator[ControlMessage]:
        """Yield the frozen FIFO and byte allowance before checking for EOF."""
        for _ in range(queued):
            yield self._messages.popleft()
        if snapshot_error is not None:
            self._poison()
            raise EOFError('worker control is closed') from snapshot_error
        while remaining:
            try:
                data = sock.recv(min(65_536, remaining))
            except (OSError, ValueError) as error:
                self._poison()
                raise EOFError('worker control is closed') from error
            if not data:
                self._poison()
                raise EOFError('worker control peer closed')
            remaining -= len(data)
            bodies = self._reader._feed(data, frame_limit=1)
            while bodies:
                yield _decode_control_body(bodies[0])
                bodies = self._reader._feed(b'', frame_limit=1)
        try:
            data = sock.recv(1, socket.MSG_PEEK)
        except BlockingIOError:
            return
        except (OSError, ValueError) as error:
            self._poison()
            raise EOFError('worker control is closed') from error
        if not data:
            self._poison()
            raise EOFError('worker control peer closed')

    @staticmethod
    def _wait_ready(
        sock: socket.socket,
        event: int,
        deadline: float,
        *,
        allow_expired_ready: bool = False,
        timeout_message: str,
    ) -> None:
        """Wait for advisory readiness within one absolute deadline."""
        timeout_error = (
            WorkerControlSendTimeoutError if event == selectors.EVENT_WRITE else TimeoutError
        )
        while True:
            now = time.monotonic()
            if now >= deadline and not allow_expired_ready:
                raise timeout_error(timeout_message)
            remaining = max(0.0, deadline - now)
            with selectors.DefaultSelector() as selector:
                selector.register(sock, event)
                ready = selector.select(remaining)
            if ready:
                if time.monotonic() >= deadline and not allow_expired_ready:
                    raise timeout_error(timeout_message)
                return
            allow_expired_ready = False
            if time.monotonic() >= deadline:
                raise timeout_error(timeout_message)

    def _send_frame(self, sock: socket.socket, frame: bytes, deadline: float) -> None:
        """Queue one frame through bounded nonblocking partial writes."""
        unsent = memoryview(frame)
        while unsent:
            self._wait_ready(
                sock,
                selectors.EVENT_WRITE,
                deadline,
                timeout_message='worker control send timed out',
            )
            try:
                sent = sock.send(unsent)
            except BlockingIOError:
                continue
            if sent == 0:
                raise EOFError('worker control peer closed')
            unsent = unsent[sent:]

    def close(self) -> None:
        """Close the control descriptor idempotently."""
        self._poison()

    @classmethod
    def from_fd(
        cls,
        fd: int,
        *,
        max_frame_bytes: int,
        send_deadline_secs: float,
    ) -> Self:
        """Take ownership of one inherited control descriptor.

        Args:
            fd: Nonnegative inherited connected Unix socket descriptor.
            max_frame_bytes: Positive maximum control frame body bytes.
            send_deadline_secs: Finite positive complete-operation send
                deadline in seconds.

        Returns:
            A control channel owning `fd`.

        Raises:
            ValueError: The descriptor, socket family, or channel bound is
                invalid.
        """
        if type(fd) is not int or fd < 0:
            raise ValueError('control descriptor must be nonnegative')
        return cls(
            socket.socket(fileno=fd),
            max_frame_bytes=max_frame_bytes,
            send_deadline_secs=send_deadline_secs,
        )

    def receive(self, *, timeout: float) -> ControlMessage:
        """Receive one message within one complete-operation deadline.

        Args:
            timeout: Finite nonnegative complete-operation deadline.

        Returns:
            The next typed control message.

        Raises:
            EOFError: The channel is closed or its receive transport fails.
            TimeoutError: No complete message arrived before `timeout`.
            ValueError: The timeout is invalid or the peer sent malformed data.
        """
        if not isfinite(timeout) or timeout < 0:
            raise ValueError('worker control receive timeout must be finite and nonnegative')
        sock = self._open_socket()
        if self._messages:
            return self._messages.popleft()
        deadline = time.monotonic() + timeout
        allow_expired_ready = timeout == 0.0
        while not self._messages:
            try:
                self._wait_ready(
                    sock,
                    selectors.EVENT_READ,
                    deadline,
                    allow_expired_ready=allow_expired_ready,
                    timeout_message='worker control receive timed out',
                )
                allow_expired_ready = False
                try:
                    data = sock.recv(min(65_536, self._max_frame_bytes + _LENGTH_BYTES))
                except BlockingIOError:
                    continue
            except TimeoutError:
                raise
            except (OSError, ValueError) as error:
                self._poison()
                raise EOFError('worker control is closed') from error
            if not data:
                self._poison(send_error=BrokenPipeError('worker control peer closed'))
                raise EOFError('worker control peer closed')
            for body in self._reader._feed(data):
                self._messages.append(_decode_control_body(body))
        return self._messages.popleft()

    def receive_available(self) -> Iterator[ControlMessage]:
        """Freeze and yield one finite batch of currently available messages.

        Acquisition occurs at this call, before iteration. The sole reader must
        exhaust the iterator before any other receive operation. Bytes arriving
        after acquisition belong to a later batch; partial frames remain
        buffered without waiting for completion.

        Returns:
            An ordered iterator over buffered messages and the entry socket
            byte snapshot. Earlier messages precede any later frame error or
            transport EOF raised during iteration.

        Raises:
            EOFError: The channel is closed or its transport inspection fails.
            ValueError: A received frame is invalid.
        """
        sock = self._open_socket()
        snapshot_error = None
        try:
            remaining = pending_socket_bytes(sock)
        except (OSError, ValueError) as error:
            remaining = 0
            snapshot_error = error
        return self._receive_available(sock, len(self._messages), remaining, snapshot_error)

    def send(self, message: ControlMessage) -> None:
        """Send one complete typed control message within its channel deadline.

        The deadline includes time waiting for the writer lock. A timeout or
        transport failure permanently closes and poisons this channel, so a
        retry requires a different channel.

        Args:
            message: Closed internal control value.

        Raises:
            WorkerControlSendTimeoutError: The send deadline expires while
                waiting for the writer lock or writing the frame.
            ValueError: The encoded control frame body exceeds the configured
                bound.
            EOFError: The channel or its peer is closed.
            OSError: The socket transport fails; the same failure is propagated
                to a concurrent writer after the channel is poisoned.
        """
        frame = encode_control_message(message)
        if len(frame) - _LENGTH_BYTES > self._max_frame_bytes:
            raise _FrameError('control frame body exceeds configured maximum')
        deadline = time.monotonic() + self._send_deadline_secs
        remaining = max(0.0, deadline - time.monotonic())
        if not self._send_lock.acquire(timeout=remaining):
            self._poison()
            raise WorkerControlSendTimeoutError('worker control send timed out')
        try:
            try:
                sock = self._open_socket()
            except EOFError:
                with self._state_lock:
                    send_error = self._send_error
                if send_error is not None:
                    raise type(send_error)(*send_error.args) from send_error
                raise
            self._send_frame(sock, frame, deadline)
        except TimeoutError:
            self._poison()
            raise
        except EOFError:
            self._poison()
            raise
        except OSError as error:
            self._poison(send_error=error)
            raise
        except ValueError:
            self._poison()
            raise
        finally:
            self._send_lock.release()

    @property
    def socket_buffers(self) -> SocketBufferSizes:
        """Return requested and effective kernel buffer evidence.

        Returns:
            Best-effort platform-reported values.
        """
        return self._socket_buffers


class WorkerControlSendTimeoutError(TimeoutError):
    """Identify a bounded worker-control send deadline."""


class _FrameError(ValueError):
    """Report malformed, unsupported, or oversized internal wire data."""


class _IPCAuthenticationError(PermissionError):
    """Report a worker IPC identity or capability mismatch."""


@dataclass(frozen=True)
class _OutboundFrame:
    """Hold one immutable segmented length-prefixed event frame."""

    _header: bytes
    _payload: bytes
    _charge_bytes: int
    _prefix: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Validate and cache the complete wire length prefix."""
        if type(self._header) is not bytes or type(self._payload) is not bytes:
            raise TypeError('outbound frame segments must be bytes')
        if type(self._charge_bytes) is not int or self._charge_bytes <= 0:
            raise ValueError('outbound frame charge must be positive')
        frame_length = _LENGTH_BYTES + len(self._header) + len(self._payload)
        object.__setattr__(self, '_prefix', struct.pack('!II', frame_length, len(self._header)))

    @property
    def _parts(self) -> tuple[bytes, bytes, bytes]:
        """Return immutable wire segments without copying the event body."""
        return self._prefix, self._header, self._payload

    @property
    def _wire_bytes(self) -> int:
        """Return complete bytes written to the stream."""
        return sum(len(part) for part in self._parts)


@dataclass
class _ConnectionFence:
    """Fence stale roles with one server-ordered group attempt."""

    _accepted_sequence: int
    _connection_id: str
    _retired: bool = False


@dataclass(frozen=True)
class _EventAuthentication:
    """Carry the closed event-channel handshake."""

    _capability: str
    _connection_id: str
    _group: str
    _role: Literal['coordination', 'event']
    _source: str
    _spawn_instance: str


class _EventClientOptions(TypedDict):
    """Define one authenticated dependent event-client configuration."""

    path: Path
    source: str
    group: str
    spawn_instance: str
    capability: str
    max_frame_bytes: int
    queue_max_entries: int
    queue_max_bytes: int
    coordination_deadline_secs: float
    stop: threading.Event


class _EventServerOptions(TypedDict):
    """Define one private source event-server configuration."""

    path: Path
    source: str
    max_frame_bytes: int
    queue_max_entries: int
    queue_max_bytes: int
    expected_uid: NotRequired[int | None]
    frame_encoder: NotRequired[Callable[[FanoutMessage], _OutboundFrame]]


@dataclass(frozen=True)
class _JoinCall:
    """Carry one decoded lane-to-source RPC for source-thread dispatch."""

    _request_id: int
    _value: _JoinValue


@dataclass(frozen=True)
class _JoinReply:
    """Carry one request-correlated source RPC result."""

    _request_id: int
    _result: _JoinResult
    _error: str | None = None


class _JoinSource(Protocol):
    """Define source-thread operations exposed over remote event IPC."""

    def abort(self, abort: JoinAbort) -> None:
        """Abort one exact remote join attempt.

        Args:
            abort: Exact join attempt to abort.
        """
        ...

    def acknowledge_strict_control(
        self,
        control: StrictCapacityControl | StrictOrderingControl,
    ) -> None:
        """Acknowledge one lane-committed strict control remotely.

        Args:
            control: Capacity or ordering control committed by the lane.
        """
        ...

    def confirm(self, confirmation: JoinConfirm) -> Attached | JoinRetry:
        """Confirm catch-up reached one exact remote boundary.

        Args:
            confirmation: Exact join boundary confirmation.

        Returns:
            Attached identity or retry instruction.
        """
        ...

    def event_channel_lost(self, group: str, *, reason: str) -> None:
        """Record loss of one remote event channel.

        Args:
            group: Stable group identity.
            reason: Body-free channel-loss condition.
        """
        ...

    def finalize(self, finalization: JoinFinalize) -> JoinFinalized:
        """Finalize one exact remote join attempt.

        Args:
            finalization: Exact join identity to finalize.

        Returns:
            Finalized join identity.
        """
        ...

    def observe_group(
        self,
        request: GroupBootstrapRequest,
    ) -> GroupBootstrapObservation | GroupBootstrapPending:
        """Capture one live source observation before group state opens.

        Args:
            request: Exact group bootstrap observation request.

        Returns:
            Current source observation or pending retry instruction.
        """
        ...

    def request(
        self,
        request: JoinRequest,
        channel: GroupChannelInterface,
    ) -> JoinTarget | JoinRetry:
        """Capture one remote source boundary on the authenticated channel.

        Args:
            request: Exact live-join request.
            channel: Authenticated group fanout channel.

        Returns:
            Captured join target or retry instruction.
        """
        ...


@dataclass(frozen=True)
class _PeerDetach:
    """Carry one body-free event-channel detach reason."""

    _reason: str


class _FrameReader:
    """Incrementally decode bounded event-channel frames."""

    def __init__(
        self,
        *,
        max_frame_bytes: int,
        coordination_only: bool = False,
    ) -> None:
        """Create one empty bounded reader."""
        if max_frame_bytes <= 0:
            raise ValueError('maximum frame bytes must be positive')
        self._coordination_only = coordination_only
        self._buffer = bytearray()
        self._expected: int | None = None
        self._max_frame_bytes = max_frame_bytes

    def _feed(
        self,
        data: bytes,
    ) -> tuple[FanoutMessage | _JoinReply | _PeerDetach, ...]:
        """Consume bytes and return every newly complete message."""
        if data:
            self._buffer.extend(data)
        decoded: list[FanoutMessage | _JoinReply | _PeerDetach] = []
        while True:
            expected = self._expected
            if expected is None:
                expected = _frame_length(self._buffer, max_frame_bytes=self._max_frame_bytes)
                if expected is None:
                    break
                self._expected = expected
            total = _LENGTH_BYTES + expected
            if len(self._buffer) < total:
                break
            complete = bytes(self._buffer[:total])
            del self._buffer[:total]
            self._expected = None
            decoded.append(_decode_event_wire(complete, coordination_only=self._coordination_only))
        return tuple(decoded)

    @property
    def _pending_wire_bytes(self) -> int | None:
        """Return the complete current-frame charge after its prefix."""
        if self._expected is None:
            return None
        return _LENGTH_BYTES + self._expected

    @property
    def _read_size(self) -> int:
        """Return bytes needed to finish at most one current frame."""
        if self._expected is None:
            return max(1, _LENGTH_BYTES - len(self._buffer))
        return max(1, _LENGTH_BYTES + self._expected - len(self._buffer))


class _RawFrameReader:
    """Incrementally decode bounded opaque length-prefixed frames."""

    def __init__(self, *, max_frame_bytes: int) -> None:
        """Create one empty opaque reader."""
        self._buffer = bytearray()
        self._max_frame_bytes = max_frame_bytes

    def _feed(
        self,
        data: bytes,
        *,
        frame_limit: int | None = None,
    ) -> tuple[bytes, ...]:
        """Consume bytes and return complete bodies without length prefixes."""
        self._buffer.extend(data)
        decoded: list[bytes] = []
        while frame_limit is None or len(decoded) < frame_limit:
            length = _frame_length(self._buffer, max_frame_bytes=self._max_frame_bytes)
            if length is None:
                break
            total = _LENGTH_BYTES + length
            if len(self._buffer) < total:
                break
            decoded.append(bytes(self._buffer[_LENGTH_BYTES:total]))
            del self._buffer[:total]
        return tuple(decoded)

    def _handoff(self) -> bytes:
        """Release every unread byte to a replacement reader."""
        unread = bytes(self._buffer)
        self._buffer.clear()
        return unread

    @property
    def _read_size(self) -> int:
        """Return bytes needed to finish at most one current frame."""
        if len(self._buffer) < _LENGTH_BYTES:
            return _LENGTH_BYTES - len(self._buffer)
        (length,) = struct.unpack('!I', self._buffer[:_LENGTH_BYTES])
        return max(1, _LENGTH_BYTES + length - len(self._buffer))


class _RemoteGroupChannel:
    """Expose one EventServer peer as a source-side fanout channel."""

    def __init__(
        self,
        server: EventServer,
        *,
        connection_id: str,
        group: str,
        ordering: Literal['eager', 'strict'],
    ) -> None:
        """Create one attached, nonprovisional remote channel adapter."""
        self._attached = True
        self._connection_id = connection_id
        self._group = group
        self._ordering: Literal['eager', 'strict'] = ordering
        self._provisional = False
        self._reason: str | None = None
        self._server = server

    def activate(self, *, provisional: bool) -> None:
        """Activate the current authenticated remote peer.

        Args:
            provisional: Whether the attachment awaits promotion.
        """
        self._attached = True
        self._provisional = provisional
        self._reason = None

    def detach(self, reason: str) -> None:
        """Detach and close the exact remote peer.

        Args:
            reason: Body-free detach condition.
        """
        if not self._attached:
            return
        self._attached = False
        self._provisional = False
        self._reason = reason
        self._server._close_connection(
            self._group,
            self._connection_id,
            reason=reason,
        )

    def close(self) -> None:
        """Detach and close the exact remote peer."""
        self.detach('channel-closed')

    def drain(self) -> tuple[FanoutMessage, ...]:
        """Return no source-side bodies because transport owns the queue.

        Returns:
            Empty tuple because the transport owns queued bodies.
        """
        return ()

    @property
    def group(self) -> str:
        """Return the stable remote group identity.

        Returns:
            Stable authenticated group identity.
        """
        return self._group

    def offer(self, message: FanoutMessage) -> bool:
        """Offer one message to only this authenticated peer.

        Args:
            message: Immutable fanout message.

        Returns:
            `True` while the exact remote peer remains connected.

        Raises:
            ValueError: The frame body exceeds the configured limit.
        """
        status = self._server._publish_connection(
            self._group,
            self._connection_id,
            message,
        )
        return status._connected

    def offer_hint(self, message: RangeChanged) -> bool:
        """Offer one coalesced hint without closing a saturated peer.

        Returns:
            `True` when queued or coalesced with a pending hint. `False` when
            the exact peer is unavailable or its event queue is at capacity.

        Raises:
            ValueError: The frame body exceeds the configured limit.
        """
        return self._server._offer_hint_connection(
            self._group,
            self._connection_id,
            message,
        )

    def offer_paced(self, message: FanoutMessage) -> bool:
        """Offer one strict message without detaching at current pressure.

        Args:
            message: Immutable strict fanout message.

        Returns:
            `True` when the peer queue accepts the frame; otherwise `False`.

        Raises:
            ValueError: The frame body exceeds the configured limit.
        """
        try:
            return self._server._offer_paced_connection(
                self._group,
                self._connection_id,
                message,
            )
        except ConnectionError:
            return False

    @property
    def ordering(self) -> Literal['eager', 'strict']:
        """Return eager or strict remote ordering.

        Returns:
            Configured remote ordering mode.
        """
        return self._ordering

    def promote(self) -> None:
        """Promote one provisional remote attachment.

        Raises:
            ConnectionError: The remote channel is detached.
        """
        if not self._attached:
            raise ConnectionError('cannot promote a detached remote channel')
        self._provisional = False

    @property
    def status(self) -> FanoutStatus:
        """Return current remote connection and bounded queue state.

        Returns:
            Current attachment, ordering, queue, and detach state.
        """
        status = self._server._connection_status(
            self._group,
            self._connection_id,
        )
        return FanoutStatus(
            group=self._group,
            ordering=self._ordering,
            attached=self._attached and status._connected,
            provisional=self._provisional,
            queued_count=status.queued_count,
            queued_bytes=status.queued_bytes,
            reason=self._reason if self._reason is not None else status._reason,
        )


class _SendSocket(Protocol):
    """Define the partial-write socket surface."""

    def send(self, value: memoryview, /) -> int:
        """Write a nonempty prefix and return its length.

        Args:
            value: Remaining immutable frame view.

        Returns:
            Number of bytes accepted by the socket.
        """
        ...


class _PartialWriter:
    """Write immutable segmented frames across arbitrary short writes."""

    def __init__(
        self,
        *,
        max_entries: int | None = None,
        max_bytes: int | None = None,
    ) -> None:
        """Create one empty optionally bounded writer."""
        self._frames: deque[_OutboundFrame] = deque()
        self._hint_frame: _OutboundFrame | None = None
        self._charged_count = 0
        self._max_bytes = max_bytes
        self._max_entries = max_entries
        self._offset = 0
        self._part = 0
        self._pending_byte_count = 0
        self._last_rejection_reason: str | None = None

    def _append(self, frame: _OutboundFrame) -> bool:
        """Append one immutable frame within count and byte limits."""
        self._last_rejection_reason = None
        if self._max_entries is not None and self._charged_count + 1 > self._max_entries:
            self._last_rejection_reason = 'queue-count-exhausted'
            return False
        if (
            self._max_bytes is not None
            and self._pending_byte_count + frame._charge_bytes > self._max_bytes
        ):
            self._last_rejection_reason = 'queue-byte-exhausted'
            return False
        self._frames.append(frame)
        self._charged_count += 1
        self._pending_byte_count += frame._charge_bytes
        return True

    def _append_hint(self, frame: _OutboundFrame) -> bool:
        """Coalesce one advisory frame outside charged writer capacity."""
        if self._hint_frame is not None:
            return True
        if (self._max_entries is not None and self._charged_count >= self._max_entries) or (
            self._max_bytes is not None and self._pending_byte_count >= self._max_bytes
        ):
            return False
        self._frames.append(frame)
        self._hint_frame = frame
        return True

    def _clear(self) -> None:
        """Discard every queued frame and reset partial-write state."""
        self._frames.clear()
        self._hint_frame = None
        self._charged_count = 0
        self._offset = 0
        self._part = 0
        self._pending_byte_count = 0
        self._last_rejection_reason = None

    def _finish_frame(self, frame: _OutboundFrame) -> None:
        """Retire one completed frame from its own accounting domain."""
        self._frames.popleft()
        if frame is self._hint_frame:
            self._hint_frame = None
        else:
            self._charged_count -= 1
            self._pending_byte_count -= frame._charge_bytes

    def _flush(self, sock: _SendSocket) -> None:
        """Advance queued frames until complete or the socket would block."""
        while self._frames:
            frame = self._frames[0]
            parts = frame._parts
            while self._part < len(parts) and self._offset == 0 and not parts[self._part]:
                self._part += 1
            if self._part == len(parts):
                self._part = 0
                self._finish_frame(frame)
                continue
            current = parts[self._part]
            try:
                written = sock.send(memoryview(current)[self._offset :])
            except (BlockingIOError, InterruptedError):
                return
            if written <= 0:
                raise ConnectionError('socket partial write made no progress')
            self._offset += written
            if self._offset < len(current):
                continue
            self._offset = 0
            self._part += 1
            if self._part < len(parts):
                continue
            self._part = 0
            self._finish_frame(frame)

    @property
    def _pending_bytes(self) -> int:
        """Return charged bytes retained by queued frames."""
        return self._pending_byte_count

    @property
    def _pending_count(self) -> int:
        """Return the queued charged-frame count, excluding the advisory hint."""
        return self._charged_count

    @property
    def _has_pending(self) -> bool:
        """Return whether any charged or advisory frame awaits writing."""
        return bool(self._frames)

    @property
    def _rejection_reason(self) -> str | None:
        """Return the last body-free capacity rejection reason."""
        return self._last_rejection_reason

    @property
    def _started(self) -> bool:
        """Return whether any queued frame has been partially written."""
        return self._part != 0 or self._offset != 0


@dataclass
class _ServerPeer:
    """Hold one accepted event socket and bounded writer."""

    _socket: socket.socket
    _buffers: SocketBufferSizes
    _reader: _RawFrameReader
    _writer: _PartialWriter
    _accepted_sequence: int
    _active_request_id: int | None = None
    _grant: EventPeerGrant | None = None
    _connection_id: str | None = None
    _group: str | None = None
    _role: Literal['coordination', 'event'] | None = None
    _close_after_flush: str | None = None


class EventClient:
    """Own a dependent lane's paired event and coordination transports.

    The caller drives event service with `poll`, and at most one coordination
    call may be in flight. `request` retains a caller-owned lane channel as the
    active event sink; teardown detaches that borrowed channel and closes all
    client-owned sockets and selectors.

    A coordination timeout closes the transport without proving that a
    dispatched source mutation was cancelled. A retry must therefore preserve
    the exact operation identity.
    """

    def __init__(self, **options: Unpack[_EventClientOptions]) -> None:
        """Configure one unopened event client.

        Args:
            path: Source-owned Unix endpoint.
            source: Required nonempty source identity that must match the
                endpoint and supervisor-issued grant.
            group: Stable dependent lane identity.
            spawn_instance: Current supervisor-minted process identity.
            capability: Supervisor-issued per-spawn endpoint capability.
            max_frame_bytes: Maximum event frame body bytes.
            queue_max_entries: Lane queue message bound.
            queue_max_bytes: Lane queue complete-frame byte bound.
            coordination_deadline_secs: Maximum duration of one coordination
                call.
            stop: Owning caller's shutdown signal.

        Raises:
            ValueError: An identity is empty or a frame or queue bound is not
                positive.
        """
        path = options['path']
        source = options['source']
        group = options['group']
        spawn_instance = options['spawn_instance']
        capability = options['capability']
        max_frame_bytes = options['max_frame_bytes']
        queue_max_entries = options['queue_max_entries']
        queue_max_bytes = options['queue_max_bytes']
        coordination_deadline_secs = options['coordination_deadline_secs']
        stop = options['stop']
        if not source or not group or not spawn_instance or not capability:
            raise ValueError('event client identity must not be empty')
        if max_frame_bytes <= 0 or queue_max_entries <= 0 or queue_max_bytes <= 0:
            raise ValueError('event client bounds must be positive')
        self._capability = capability
        self._needs_catchup = False
        self._detach_reason: str | None = None
        self._group = group
        self._max_frame_bytes = max_frame_bytes
        self._path = path
        self._queue_max_bytes = queue_max_bytes
        self._queue_max_entries = queue_max_entries
        self._reader = _FrameReader(max_frame_bytes=max_frame_bytes)
        self._coordination_reader = _FrameReader(
            max_frame_bytes=max_frame_bytes,
            coordination_only=True,
        )
        self._coordination_selector: selectors.BaseSelector | None = None
        self._coordination_socket: socket.socket | None = None
        self._coordination_deadline_secs = coordination_deadline_secs
        self._coordination_writer = _PartialWriter(max_entries=1)
        self._in_flight_request_id: int | None = None
        self._reply: _JoinReply | None = None
        self._request_id = 0
        self._selector: selectors.BaseSelector | None = None
        self._socket: socket.socket | None = None
        self._socket_buffers = SocketBufferSizes(
            requested=max_frame_bytes,
            send=0,
            receive=0,
        )
        self._join_channel: GroupChannelInterface | None = None
        self._last_join_target: JoinTarget | None = None
        self._source = source
        self._spawn_instance = spawn_instance
        self._stop = stop

    def _detach(self, reason: str) -> None:
        """Detach and discard every stale event body."""
        channel = self._join_channel
        self._join_channel = None
        first_error: BaseException | None = None
        resources = (
            self._selector,
            self._coordination_selector,
            self._socket,
            self._coordination_socket,
        )
        self._selector = None
        self._coordination_selector = None
        self._socket = None
        self._coordination_socket = None
        try:
            close_resources(resources)
        except BaseException as error:  # noqa: BLE001
            first_error = error
        self._coordination_writer._clear()
        self._in_flight_request_id = None
        self._reply = None
        self._coordination_reader = _FrameReader(
            max_frame_bytes=self._max_frame_bytes,
            coordination_only=True,
        )
        self._reader = _FrameReader(max_frame_bytes=self._max_frame_bytes)
        self._needs_catchup = True
        self._detach_reason = reason
        if channel is not None:
            close_callables(
                (partial(channel.detach, reason),),
                primary_error=first_error,
                failure_note_prefix='additional cleanup failure',
            )
        if first_error is not None:
            raise first_error

    def _queue_decoded(
        self,
        decoded: tuple[FanoutMessage | _JoinReply | _PeerDetach, ...],
    ) -> int:
        """Queue decoded messages until a detach or capacity boundary."""
        added = 0
        for message in decoded:
            if isinstance(message, _JoinReply):
                self._detach('unexpected-join-reply')
                raise JoinProtocolError('event stream received a join reply')
            if isinstance(message, _PeerDetach):
                self._detach('unexpected-event-disconnect')
                raise JoinProtocolError('event stream received a disconnect')
            channel = self._join_channel
            accepted = (
                channel.offer_hint(message)
                if channel is not None and isinstance(message, RangeChanged)
                else channel.offer(message)
                if channel is not None
                else False
            )
            if not accepted and isinstance(message, RangeChanged):
                continue
            if not accepted:
                self._detach('lane-queue-exhausted')
                break
            added += 1
        return added

    def _queue_reply(
        self,
        decoded: tuple[FanoutMessage | _JoinReply | _PeerDetach, ...],
    ) -> None:
        """Retain one exact reply or fail closed on coordination input."""
        for message in decoded:
            if isinstance(message, _PeerDetach):
                self._detach(message._reason)
                raise ConnectionError('event coordination channel disconnected')
            if not isinstance(message, _JoinReply):
                self._detach('unexpected-coordination-frame')
                raise JoinProtocolError('coordination stream received a fanout frame')
            request_id = self._in_flight_request_id
            if request_id is None:
                self._detach('unsolicited-join-reply')
                raise JoinProtocolError('unsolicited join reply')
            if message._request_id != request_id:
                self._detach('wrong-id-join-reply')
                raise JoinProtocolError('join reply request id conflicts')
            if self._reply is not None:
                self._detach('duplicate-join-reply')
                raise JoinProtocolError('duplicate join reply')
            self._reply = message

    def _read_paused(self) -> bool:
        """Return whether the current queue cannot accept another frame."""
        channel = self._join_channel
        status = channel.status if channel is not None else None
        queued_count = status.queued_count if status is not None else 0
        queued_bytes = status.queued_bytes if status is not None else 0
        pending_wire_bytes = self._reader._pending_wire_bytes
        return (
            queued_count >= self._queue_max_entries
            or queued_bytes >= self._queue_max_bytes
            or (
                pending_wire_bytes is not None
                and (
                    queued_count + 1 > self._queue_max_entries
                    or queued_bytes + pending_wire_bytes > self._queue_max_bytes
                )
            )
        )

    @staticmethod
    def _remaining_connection_timeout(deadline: float) -> float:
        """Return the positive time left for the private startup sockets."""
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('event endpoint connection timed out')
        return remaining

    @staticmethod
    def _send_authentication(
        sock: socket.socket,
        frame: bytes,
        deadline: float,
    ) -> None:
        """Write one complete authentication frame within the shared deadline."""
        pending = memoryview(frame)
        while pending:
            sock.settimeout(EventClient._remaining_connection_timeout(deadline))
            written = sock.send(pending)
            if written == 0:
                raise ConnectionError('event endpoint closed during authentication')
            pending = pending[written:]

    def _set_coordination_interest(self) -> None:
        """Add write interest exactly while one request remains pending."""
        selector = self._coordination_selector
        sock = self._coordination_socket
        if selector is None or sock is None:
            return
        events = selectors.EVENT_READ
        if self._coordination_writer._pending_count:
            events |= selectors.EVENT_WRITE
        selector.modify(sock, events)

    def _poll_coordination(self, timeout: float) -> None:
        """Receive one bounded coordination frame without touching events."""
        selector = self._coordination_selector
        sock = self._coordination_socket
        if selector is None or sock is None:
            raise ConnectionError('event client is not connected')
        for _key, mask in selector.select(timeout):
            if mask & selectors.EVENT_WRITE:
                try:
                    self._coordination_writer._flush(sock)
                except (ConnectionError, OSError) as error:
                    self._detach('coordination-write-failed')
                    raise ConnectionError('event coordination channel write failed') from error
                self._set_coordination_interest()
            if not mask & selectors.EVENT_READ:
                continue
            try:
                data = sock.recv(min(65_536, self._coordination_reader._read_size))
            except BlockingIOError:
                continue
            except OSError as error:
                self._detach('coordination-read-failed')
                raise ConnectionError('event coordination channel read failed') from error
            if not data:
                self._detach('coordination-eof')
                raise ConnectionError('event coordination channel closed')
            try:
                decoded = self._coordination_reader._feed(data)
            except _FrameError as error:
                self._detach('malformed-coordination-frame')
                raise JoinProtocolError('malformed coordination frame') from error
            self._queue_reply(decoded)

    def _rpc(
        self,
        value: _JoinValue,
    ) -> _JoinResult:
        """Send one bounded call and wait for its exact correlated reply."""
        deadline = time.monotonic() + self._coordination_deadline_secs
        sock = self._coordination_socket
        if sock is None:
            raise ConnectionError('event client is not connected')
        if self._in_flight_request_id is not None:
            self._detach('concurrent-join-request')
            raise JoinProtocolError('event client already has an in-flight request')
        self._request_id += 1
        request_id = self._request_id
        frame = _encode_join_call(
            _JoinCall(
                _request_id=request_id,
                _value=value,
            )
        )
        if frame._wire_bytes - _LENGTH_BYTES > self._max_frame_bytes:
            raise _FrameError('join request frame body exceeds configured maximum')
        self._in_flight_request_id = request_id
        self._reply = None
        try:
            if not self._coordination_writer._append(frame):
                self._detach('coordination-request-queue-exhausted')
                raise JoinProtocolError('event coordination request queue is full')
            self._set_coordination_interest()
            while self._coordination_socket is not None:
                reply = self._reply
                if reply is not None:
                    self._reply = None
                    if reply._error is not None:
                        raise JoinProtocolError(reply._error)
                    return reply._result
                if self._stop.is_set():
                    self._detach('coordination-stop')
                    raise ConnectionError('event channel stopped during join request')
                if time.monotonic() >= deadline:
                    self._detach('coordination-reply-timeout')
                    raise ConnectionError('event coordination reply timed out')
                self._poll_coordination(0.1)
            raise ConnectionError('event channel closed during join request')
        finally:
            self._in_flight_request_id = None
            self._reply = None

    def _service_disconnect_before_event_loss(self) -> bool:
        """Prefer an exact coordination disconnect over event EOF."""
        with suppress(ConnectionError):
            self._poll_coordination(0.0)
        return self._socket is None

    def _read_event(self, sock: socket.socket) -> int:
        """Read and queue at most one bounded event frame."""
        while True:
            try:
                data = sock.recv(min(65_536, self._reader._read_size))
            except BlockingIOError:
                return 0
            except OSError as error:
                if self._service_disconnect_before_event_loss():
                    return 0
                self._detach('read-failed')
                raise ConnectionError('event channel read failed') from error
            if not data:
                if not self._service_disconnect_before_event_loss():
                    self._detach('eof')
                return 0
            try:
                decoded = self._reader._feed(data)
            except _FrameError:
                self._detach('malformed-frame')
                return 0
            if decoded:
                return self._queue_decoded(decoded)
            if self._read_paused():
                return 0

    def _set_read_interest(self) -> None:
        """Pause reads exactly while the bounded lane queue is full."""
        selector = self._selector
        sock = self._socket
        if selector is None or sock is None:
            return
        full = self._read_paused()
        registered = bool(selector.get_map())
        if full and registered:
            selector.unregister(sock)
        elif not full and not registered:
            selector.register(sock, selectors.EVENT_READ)

    def abort(self, abort: JoinAbort) -> None:
        """Abort one exact remote join attempt.

        Args:
            abort: Exact join attempt to abort.

        Raises:
            ConnectionError: The coordination channel is unavailable, fails,
                stops, or times out.
            ValueError: The encoded request exceeds the configured frame limit.
            JoinProtocolError: The coordination protocol fails or reports a
                remote error.
        """
        self._rpc(abort)

    def acknowledge_strict_control(
        self,
        control: StrictCapacityControl | StrictOrderingControl,
    ) -> None:
        """Acknowledge one lane-committed strict control remotely.

        Args:
            control: Capacity or ordering control committed by the lane.

        Raises:
            ConnectionError: The coordination channel is unavailable, fails,
                stops, or times out.
            ValueError: The encoded request exceeds the configured frame limit.
            JoinProtocolError: The coordination protocol fails or reports a
                remote error.
        """
        self._rpc(control)

    def close(self) -> None:
        """Close the event channel and release stale bodies."""
        self._detach('client-closed')

    def confirm(self, confirmation: JoinConfirm) -> Attached | JoinRetry:
        """Confirm catch-up reached one exact remote boundary.

        Args:
            confirmation: Exact join boundary confirmation.

        Returns:
            Attached identity or retry instruction.

        Raises:
            ConnectionError: The coordination channel is unavailable, fails,
                stops, or times out.
            ValueError: The encoded request exceeds the configured frame limit.
            JoinProtocolError: The coordination protocol or reply type
                conflicts.
        """
        result = self._rpc(confirmation)
        if not isinstance(result, Attached | JoinRetry):
            raise JoinProtocolError('remote confirm reply conflicts')
        return result

    @property
    def connected(self) -> bool:
        """Return whether the event socket remains connected.

        Returns:
            Current transport state.
        """
        return self._socket is not None and self._coordination_socket is not None

    def connect(self) -> None:
        """Connect both socket roles and send their authentication frames.

        Return does not wait for server-side grant acceptance. A later
        coordination operation detects rejection.

        Raises:
            ConnectionError: Either transport cannot connect or send its
                authentication frame.
        """
        if self.connected:
            return
        connection_id = uuid.uuid4().hex
        event_socket: socket.socket | None = None
        coordination_socket: socket.socket | None = None
        selector: selectors.BaseSelector | None = None
        coordination_selector: selectors.BaseSelector | None = None
        deadline = time.monotonic() + self._coordination_deadline_secs
        try:
            event_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            coordination_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            socket_buffers = _request_socket_buffers(
                event_socket,
                self._max_frame_bytes,
            )
            _request_socket_buffers(
                coordination_socket,
                self._max_frame_bytes,
            )
            event_socket.settimeout(self._remaining_connection_timeout(deadline))
            event_socket.connect(os.fspath(self._path))
            event_authentication = _auth_body(
                _EventAuthentication(
                    _capability=self._capability,
                    _connection_id=connection_id,
                    _group=self._group,
                    _role='event',
                    _source=self._source,
                    _spawn_instance=self._spawn_instance,
                )
            )
            self._send_authentication(
                event_socket,
                struct.pack('!I', len(event_authentication)) + event_authentication,
                deadline,
            )
            coordination_socket.settimeout(self._remaining_connection_timeout(deadline))
            coordination_socket.connect(os.fspath(self._path))
            coordination_authentication = _auth_body(
                _EventAuthentication(
                    _capability=self._capability,
                    _connection_id=connection_id,
                    _group=self._group,
                    _role='coordination',
                    _source=self._source,
                    _spawn_instance=self._spawn_instance,
                )
            )
            self._send_authentication(
                coordination_socket,
                struct.pack('!I', len(coordination_authentication)) + coordination_authentication,
                deadline,
            )
            event_socket.settimeout(0.0)
            coordination_socket.settimeout(0.0)
            selector = selectors.DefaultSelector()
            selector.register(event_socket, selectors.EVENT_READ)
            coordination_selector = selectors.DefaultSelector()
            coordination_selector.register(coordination_socket, selectors.EVENT_READ)
        except BaseException as error:
            close_resources(
                (selector, coordination_selector, event_socket, coordination_socket),
                primary_error=error,
            )
            if isinstance(error, OSError):
                raise ConnectionError('event endpoint connection failed') from error
            raise
        self._coordination_selector = coordination_selector
        self._coordination_socket = coordination_socket
        self._socket = event_socket
        self._selector = selector
        self._socket_buffers = socket_buffers
        self._detach_reason = None

    @property
    def detach_reason(self) -> str | None:
        """Return the last body-free detach reason.

        Returns:
            Channel-loss condition, otherwise `None`.
        """
        return self._detach_reason

    def finalize(self, finalization: JoinFinalize) -> JoinFinalized:
        """Finalize one exact remote join attempt.

        Args:
            finalization: Exact join identity to finalize.

        Returns:
            Finalized join identity.

        Raises:
            ConnectionError: The coordination channel is unavailable, fails,
                stops, or times out.
            ValueError: The encoded request exceeds the configured frame limit.
            JoinProtocolError: The coordination protocol or reply type
                conflicts.
        """
        result = self._rpc(finalization)
        if not isinstance(result, JoinFinalized):
            raise JoinProtocolError('remote finalize reply conflicts')
        return result

    @property
    def last_join_target(self) -> JoinTarget | None:
        """Return the last captured direct source observation.

        Returns:
            Current join target or `None` after a retry.
        """
        return self._last_join_target

    def observe_group(
        self,
        request: GroupBootstrapRequest,
    ) -> GroupBootstrapObservation | GroupBootstrapPending:
        """Capture one live source observation before group state opens.

        Args:
            request: Exact group bootstrap observation request.

        Returns:
            Current source observation or pending retry instruction.

        Raises:
            ConnectionError: The coordination channel is unavailable, fails,
                stops, or times out.
            ValueError: The encoded request exceeds the configured frame limit.
            JoinProtocolError: The coordination protocol or bootstrap result
                conflicts.
        """
        result = self._rpc(request)
        if isinstance(result, GroupBootstrapPending):
            if (
                result.source != request.source
                or result.group != request.group
                or result.group_generation != request.group_generation
            ):
                raise JoinProtocolError('group bootstrap pending identity conflicts')
            return result
        if not isinstance(result, GroupBootstrapObservation):
            raise JoinProtocolError('group bootstrap observation reply conflicts')
        return result

    def poll(self, timeout: float = 0.0) -> int:
        """Receive at most one bounded frame while queue space remains.

        Args:
            timeout: Maximum selector wait in seconds.

        Returns:
            Number of messages accepted by the active lane sink. Zero can also
            accompany a disconnect, so callers must inspect `connected`.
        """
        if self._coordination_socket is not None:
            try:
                self._poll_coordination(0.0)
            except ConnectionError:
                return 0
        selector = self._selector
        sock = self._socket
        if selector is None or sock is None:
            return 0
        self._set_read_interest()
        if not selector.get_map():
            return 0
        added = 0
        for _key, _mask in selector.select(timeout):
            added += self._read_event(sock)
            if self._socket is None:
                break
        self._set_read_interest()
        return added

    def reconnect(self) -> None:
        """Discard stale bodies and establish a fresh authenticated channel."""
        self._detach('reconnect')
        self.connect()

    def request(
        self,
        request: JoinRequest,
        channel: GroupChannelInterface,
    ) -> JoinTarget | JoinRetry:
        """Capture one remote source boundary on the authenticated channel.

        The supplied channel becomes the borrowed event sink before the
        coordination call completes. Client teardown later detaches it.

        Args:
            request: Exact live-join request.
            channel: Authenticated group fanout channel.

        Returns:
            Captured join target or retry instruction.

        Raises:
            ConnectionError: The coordination channel is unavailable, fails,
                stops, or times out.
            ValueError: The encoded request exceeds the configured frame limit.
            JoinProtocolError: The channel identity, coordination protocol, or
                reply conflicts.
        """
        if channel.group != self._group or channel.ordering != request.attempt.join_kind:
            raise JoinProtocolError('remote join channel identity conflicts')
        self._join_channel = channel
        result = self._rpc(request)
        if not isinstance(result, JoinTarget | JoinRetry):
            raise JoinProtocolError('remote request reply conflicts')
        self._last_join_target = result if isinstance(result, JoinTarget) else None
        return result

    @property
    def socket_buffers(self) -> SocketBufferSizes:
        """Return event-socket buffer evidence in bytes.

        Returns:
            Requested and effective kernel values for the event socket, with
            zero effective values before a successful connection.
        """
        return self._socket_buffers


class EventServer:
    """Own one private authenticated reconnectable source event endpoint.

    `start` and context entry create the listener, selector, and socket path.
    Later `poll` calls accept peer sockets; `close` releases all of those owned
    resources. Each accepted client uses separate event and coordination roles
    for one exact source, group, spawn, and grant identity.

    The caller drives transport work with `poll`, then calls
    `dispatch_join_requests` on the source thread to execute queued work and
    queue replies. The server borrows that source authority for the call.
    """

    def __init__(self, **options: Unpack[_EventServerOptions]) -> None:
        """Configure one unopened private event endpoint.

        Args:
            path: Exact Unix socket path owned by this source worker.
            source: Stable source identity owned by this endpoint.
            max_frame_bytes: Maximum event frame body bytes.
            queue_max_entries: Per-group queued message bound.
            queue_max_bytes: Per-group complete-frame byte bound.
            expected_uid: Required peer UID, defaulting to this process.
            frame_encoder: Injected single-pass fanout framer.

        Raises:
            ValueError: The source is empty or a frame or queue bound is not
                positive.
        """
        path = options['path']
        source = options['source']
        max_frame_bytes = options['max_frame_bytes']
        queue_max_entries = options['queue_max_entries']
        queue_max_bytes = options['queue_max_bytes']
        if not source:
            raise ValueError('event server identity must not be empty')
        if max_frame_bytes <= 0 or queue_max_entries <= 0 or queue_max_bytes <= 0:
            raise ValueError('event server bounds must be positive')
        self._bound_identity: tuple[int, int] | None = None
        self._connection_fences: dict[str, _ConnectionFence] = {}
        expected_uid = options.get('expected_uid')
        self._expected_uid = os.geteuid() if expected_uid is None else expected_uid
        self._frame_encoder = options.get('frame_encoder', _encode_fanout_message)
        self._listener: socket.socket | None = None
        self._lost_groups: deque[str] = deque()
        self._max_frame_bytes = max_frame_bytes
        self._next_accepted_sequence = 0
        self._path = path
        self._peers: dict[int, _ServerPeer] = {}
        self._requests: deque[tuple[_ServerPeer, _JoinCall]] = deque()
        self._grants: dict[str, EventPeerGrant] = {}
        self._queue_max_bytes = queue_max_bytes
        self._queue_max_entries = queue_max_entries
        self._selector: selectors.BaseSelector | None = None
        self._socket_buffers = SocketBufferSizes(
            requested=max_frame_bytes,
            send=0,
            receive=0,
        )
        self._source = source
        self._statuses: dict[str, EventConnectionStatus] = {}

    def __enter__(self) -> Self:
        """Open the private endpoint.

        Returns:
            This live event server.
        """
        self.start()
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: object,
    ) -> None:
        """Close the private endpoint."""
        self.close()

    def _accept(self) -> None:
        """Accept every currently pending Unix peer."""
        listener = self._listener
        selector = self._selector
        if listener is None or selector is None:
            return
        while True:
            try:
                connected, _address = listener.accept()
            except BlockingIOError:
                return
            self._next_accepted_sequence += 1
            connected.settimeout(0.0)
            buffers = _request_socket_buffers(connected, self._max_frame_bytes)
            self._socket_buffers = buffers
            peer = _ServerPeer(
                _socket=connected,
                _buffers=buffers,
                _reader=_RawFrameReader(max_frame_bytes=_MAX_AUTH_FRAME_BYTES),
                _writer=_PartialWriter(max_entries=1),
                _accepted_sequence=self._next_accepted_sequence,
            )
            self._peers[connected.fileno()] = peer
            selector.register(connected, selectors.EVENT_READ, peer)

    def _close_peer(self, peer: _ServerPeer) -> None:
        """Close one exact descriptor without affecting a sibling."""
        descriptor = peer._socket.fileno()
        selector = self._selector

        def unregister() -> None:
            if selector is not None:
                with suppress(KeyError, ValueError):
                    selector.unregister(peer._socket)

        def forget() -> None:
            self._peers.pop(descriptor, None)

        close_callables((unregister, forget, peer._socket.close))

    def _connection_peer(
        self,
        group: str,
        connection_id: str,
        role: Literal['coordination', 'event'],
    ) -> _ServerPeer | None:
        """Return one exact authenticated connection role."""
        return next(
            (
                item
                for item in self._peers.values()
                if item._group == group
                and item._connection_id == connection_id
                and item._role == role
            ),
            None,
        )

    def _close_pair(self, peer: _ServerPeer, *, reason: str) -> None:
        """Close only one exact authenticated sibling pair."""
        group = peer._group
        connection_id = peer._connection_id
        if group is None or connection_id is None:
            self._close_peer(peer)
            return
        was_paired = (
            self._connection_peer(group, connection_id, 'event') is not None
            and self._connection_peer(group, connection_id, 'coordination') is not None
        )
        siblings = tuple(
            item
            for item in self._peers.values()
            if item._group == group and item._connection_id == connection_id
        )
        if not siblings:
            return
        first_error: BaseException | None = None
        try:
            close_callables(partial(self._close_peer, sibling) for sibling in siblings)
        except BaseException as error:  # noqa: BLE001
            first_error = error
        fence = self._connection_fences.get(group)
        if fence is not None and not fence._retired and fence._connection_id == connection_id:
            fence._retired = True
        if was_paired:
            self._lost_groups.append(group)
        if not any(item._group == group for item in self._peers.values()):
            self._statuses[group] = EventConnectionStatus(
                _group=group,
                _connected=False,
                queued_count=0,
                queued_bytes=0,
                _reason=reason,
            )
        if first_error is not None:
            raise first_error

    def _current_event_peer(self, group: str) -> _ServerPeer | None:
        """Return the current fully paired event peer for one group."""
        for peer in self._peers.values():
            connection_id = peer._connection_id
            if peer._group != group or peer._role != 'event' or connection_id is None:
                continue
            coordination = self._connection_peer(group, connection_id, 'coordination')
            if coordination is not None:
                return peer
        return None

    def _events(self, peer: _ServerPeer) -> int:
        """Return selector interests for one peer."""
        events = selectors.EVENT_READ
        if peer._writer._has_pending:
            events |= selectors.EVENT_WRITE
        return events

    def _fence_authentication(
        self,
        peer: _ServerPeer,
        authentication: _EventAuthentication,
    ) -> None:
        """Reject stale roles and advance one bounded group fence."""
        group = authentication._group
        connection_id = authentication._connection_id
        fence = self._connection_fences.get(group)
        if fence is None:
            self._connection_fences[group] = _ConnectionFence(
                _accepted_sequence=peer._accepted_sequence,
                _connection_id=connection_id,
            )
            return
        if fence._connection_id == connection_id:
            if fence._retired:
                raise _IPCAuthenticationError('event connection identity was already used')
            fence._accepted_sequence = max(
                fence._accepted_sequence,
                peer._accepted_sequence,
            )
            return
        if peer._accepted_sequence <= fence._accepted_sequence:
            raise _IPCAuthenticationError('event connection role is stale')
        current = next(
            (
                item
                for item in self._peers.values()
                if item._group == group and item._connection_id == fence._connection_id
            ),
            None,
        )
        if current is not None:
            self._close_pair(current, reason='peer-reconnected')
        self._connection_fences[group] = _ConnectionFence(
            _accepted_sequence=peer._accepted_sequence,
            _connection_id=connection_id,
        )

    def _authenticate(self, peer: _ServerPeer, body: bytes) -> None:
        """Authenticate one pending peer exactly once."""
        if peer._group is not None:
            raise _IPCAuthenticationError('authenticated event peer sent input')
        authentication = _decode_auth_body(body)
        identity = peer_identity(peer._socket)
        if identity.uid != self._expected_uid:
            raise _IPCAuthenticationError('event peer user conflicts')
        grant = self._grants.get(authentication._group)
        if (
            grant is None
            or authentication._source != self._source
            or authentication._source != grant.source
            or authentication._spawn_instance != grant.dependent_spawn_instance
            or authentication._capability != grant.capability
        ):
            raise _IPCAuthenticationError('event peer grant conflicts')
        self._fence_authentication(peer, authentication)
        same_role = self._connection_peer(
            authentication._group,
            authentication._connection_id,
            authentication._role,
        )
        if same_role is not None:
            self._close_pair(same_role, reason='duplicate-peer-role')
            raise _IPCAuthenticationError('event connection role is duplicated')
        peer._grant = grant
        peer._connection_id = authentication._connection_id
        peer._group = authentication._group
        peer._role = authentication._role
        peer._writer = (
            _PartialWriter(max_entries=1)
            if authentication._role == 'coordination'
            else _PartialWriter(
                max_entries=self._queue_max_entries,
                max_bytes=self._queue_max_bytes,
            )
        )
        if (
            self._connection_peer(
                authentication._group,
                authentication._connection_id,
                'event',
            )
            is not None
            and self._connection_peer(
                authentication._group,
                authentication._connection_id,
                'coordination',
            )
            is not None
        ):
            self._statuses[authentication._group] = EventConnectionStatus(
                _group=authentication._group,
                _connected=True,
                queued_count=0,
                queued_bytes=0,
                _reason=None,
            )

    @staticmethod
    def _join_call_identity(call: _JoinCall) -> tuple[str, str]:
        """Return the source and group carried by one join lifecycle call."""
        value = call._value
        if isinstance(value, GroupBootstrapRequest):
            identity = value.source, value.group
        elif isinstance(value, JoinRequest):
            identity = value.attempt.source, value.attempt.group
        else:
            identity = value.identity.source, value.identity.group
        return identity

    def _modify_peer(self, peer: _ServerPeer) -> None:
        """Refresh selector interests for one open peer."""
        selector = self._selector
        if selector is not None and peer._socket.fileno() >= 0:
            selector.modify(peer._socket, self._events(peer), peer)

    def _close_connection(
        self,
        group: str,
        connection_id: str,
        *,
        reason: str,
    ) -> None:
        """Queue a disconnect for one exact pair without touching replacement."""
        event_peer = self._connection_peer(group, connection_id, 'event')
        coordination = self._connection_peer(group, connection_id, 'coordination')
        if event_peer is None:
            return
        if coordination is None or coordination._writer._started:
            self._close_pair(event_peer, reason=reason)
            return
        frame = _detach_frame(reason)
        coordination._writer._clear()
        if not coordination._writer._append(frame):
            self._close_pair(event_peer, reason=reason)
            return
        coordination._close_after_flush = reason
        self._modify_peer(coordination)

    def _pair_is_closing(self, event_peer: _ServerPeer) -> bool:
        """Return whether an exact pair is flushing its disconnect."""
        group = event_peer._group
        connection_id = event_peer._connection_id
        if group is None or connection_id is None:
            return True
        coordination = self._connection_peer(group, connection_id, 'coordination')
        return coordination is None or coordination._close_after_flush is not None

    def _paired_event_peer(
        self,
        group: str,
        connection_id: str,
    ) -> _ServerPeer | None:
        """Return one exact usable event role without substituting a peer."""
        event_peer = self._connection_peer(group, connection_id, 'event')
        if event_peer is None or self._pair_is_closing(event_peer):
            return None
        return event_peer

    def _offer_hint_connection(
        self,
        group: str,
        connection_id: str,
        message: RangeChanged,
    ) -> bool:
        """Queue one coalesced hint on a peer without closing it at capacity."""
        peer = self._paired_event_peer(group, connection_id)
        if peer is None:
            return False
        frame = self._frame_encoder(message)
        if frame._wire_bytes - _LENGTH_BYTES > self._max_frame_bytes:
            raise _FrameError('event frame body exceeds configured maximum')
        if not peer._writer._append_hint(frame):
            return False
        self._modify_peer(peer)
        return True

    def _offer_paced_connection(
        self,
        group: str,
        connection_id: str,
        message: FanoutMessage,
    ) -> bool:
        """Offer one strict frame only to one exact event role."""
        frame = self._frame_encoder(message)
        if frame._wire_bytes - _LENGTH_BYTES > self._max_frame_bytes:
            raise _FrameError('event frame body exceeds configured maximum')
        peer = self._paired_event_peer(group, connection_id)
        if peer is None:
            raise ConnectionError(f'event group is not connected: {group}')
        if not peer._writer._append(frame):
            return False
        self._modify_peer(peer)
        return True

    def _queue_join_reply(
        self,
        peer: _ServerPeer,
        call: _JoinCall,
        result: _JoinResult,
        error: str | None,
    ) -> None:
        """Queue one bounded correlated reply while the peer remains open."""
        if peer._socket.fileno() < 0:
            return
        frame = _encode_join_reply(
            _JoinReply(
                _request_id=call._request_id,
                _result=result,
                _error=error,
            )
        )
        if frame._wire_bytes - _LENGTH_BYTES > self._max_frame_bytes:
            self._close_pair(peer, reason='join-reply-too-large')
            return
        if not peer._writer._append(frame):
            self._close_pair(
                peer,
                reason=peer._writer._rejection_reason or 'queue-exhausted',
            )
            return
        self._modify_peer(peer)

    def _queue_peer_body(self, peer: _ServerPeer, body: bytes) -> None:
        """Authenticate one body or queue one coordination call."""
        if peer._group is None:
            self._authenticate(peer, body)
            unread = peer._reader._handoff()
            peer._reader = _RawFrameReader(max_frame_bytes=self._max_frame_bytes)
            for buffered_body in peer._reader._feed(unread):
                self._queue_peer_body(peer, buffered_body)
            return
        if peer._role != 'coordination':
            raise _IPCAuthenticationError('event peer sent coordination input')
        if peer._active_request_id is not None:
            raise _FrameError('coordination peer sent a concurrent request')
        call = _decode_join_call(body)
        peer._active_request_id = call._request_id
        self._requests.append((peer, call))

    def _read_peer(self, peer: _ServerPeer) -> None:
        """Read authentication or queue bounded join calls."""
        if peer._socket.fileno() < 0:
            return
        try:
            data = peer._socket.recv(min(65_536, peer._reader._read_size))
        except BlockingIOError:
            return
        except (BrokenPipeError, ConnectionResetError):
            self._close_pair(peer, reason='peer-eof')
            return
        except OSError as error:
            if error.errno not in _UNIX_STREAM_DISCONNECT_ERRNOS:
                raise
            self._close_pair(peer, reason='peer-eof')
            return
        if not data:
            self._close_pair(peer, reason='peer-eof')
            return
        try:
            bodies = peer._reader._feed(
                data,
                frame_limit=1 if peer._group is None else None,
            )
            for body in bodies:
                self._queue_peer_body(peer, body)
        except (_FrameError, _IPCAuthenticationError, SecurityError):
            self._close_pair(peer, reason='authentication-failed')

    def _status_from_peer(
        self,
        group: str,
        connection_id: str,
        peer: _ServerPeer | None,
    ) -> EventConnectionStatus:
        """Project queue accounting or the current fence for an exact event peer."""
        if peer is None:
            fence = self._connection_fences.get(group)
            return EventConnectionStatus(
                _group=group,
                _connected=False,
                queued_count=0,
                queued_bytes=0,
                _reason=(
                    'connection-replaced'
                    if fence is not None
                    and (fence._retired or fence._connection_id != connection_id)
                    else 'not-connected'
                ),
            )
        return EventConnectionStatus(
            _group=group,
            _connected=True,
            queued_count=peer._writer._pending_count,
            queued_bytes=peer._writer._pending_bytes,
            _reason=None,
        )

    def _connection_status(
        self,
        group: str,
        connection_id: str,
    ) -> EventConnectionStatus:
        """Return event accounting for one exact sibling pair."""
        peer = self._paired_event_peer(group, connection_id)
        return self._status_from_peer(group, connection_id, peer)

    def _group_status(self, group: str) -> EventConnectionStatus:
        """Return current or last body-free status for one group."""
        peer = self._current_event_peer(group)
        if peer is not None and peer._connection_id is not None:
            return self._connection_status(group, peer._connection_id)
        return self._statuses.get(
            group,
            EventConnectionStatus(
                _group=group,
                _connected=False,
                queued_count=0,
                queued_bytes=0,
                _reason='not-connected',
            ),
        )

    def _publish_connection(
        self,
        group: str,
        connection_id: str,
        message: FanoutMessage,
    ) -> EventConnectionStatus:
        """Publish one frame only to one exact event role."""
        frame = self._frame_encoder(message)
        if frame._wire_bytes - _LENGTH_BYTES > self._max_frame_bytes:
            raise _FrameError('event frame body exceeds configured maximum')
        peer = self._paired_event_peer(group, connection_id)
        if peer is None:
            return self._connection_status(group, connection_id)
        if not peer._writer._append(frame):
            self._close_pair(
                peer,
                reason=peer._writer._rejection_reason or 'queue-exhausted',
            )
            return self._connection_status(group, connection_id)
        self._modify_peer(peer)
        return self._connection_status(group, connection_id)

    def _unlink_owned_socket(self) -> None:
        """Unlink only the socket inode created by this server."""
        try:
            metadata = self._path.lstat()
        except FileNotFoundError:
            return
        if self._bound_identity == (metadata.st_dev, metadata.st_ino) and stat.S_ISSOCK(
            metadata.st_mode
        ):
            self._path.unlink()

    def _validate_remote_identity(
        self,
        source: str,
        group: str,
        expected_group: str,
        *,
        error_message: str,
    ) -> None:
        """Reject a join call whose source or authenticated group differs."""
        if source != self._source or group != expected_group:
            raise JoinProtocolError(error_message)

    def _dispatch_join_call(
        self,
        source: _JoinSource,
        group: str,
        connection_id: str,
        call: _JoinCall,
    ) -> _JoinResult:
        """Execute one identity-checked join call on the source thread."""
        call_source, call_group = self._join_call_identity(call)
        value = call._value
        error_message = (
            'remote bootstrap identity conflicts'
            if isinstance(value, GroupBootstrapRequest)
            else 'remote join identity conflicts'
        )
        self._validate_remote_identity(
            call_source,
            call_group,
            group,
            error_message=error_message,
        )
        result: _JoinResult = None
        if isinstance(value, GroupBootstrapRequest):
            result = source.observe_group(value)
        elif isinstance(value, JoinRequest):
            channel: GroupChannelInterface = _RemoteGroupChannel(
                self,
                connection_id=connection_id,
                group=group,
                ordering=value.attempt.join_kind,
            )
            result = source.request(value, channel)
        elif isinstance(value, JoinConfirm):
            result = source.confirm(value)
        elif isinstance(value, JoinFinalize):
            result = source.finalize(value)
        elif isinstance(value, JoinAbort):
            source.abort(value)
        else:
            source.acknowledge_strict_control(value)
        return result

    def close(self) -> None:
        """Close peers, listener, selector, and exact owned socket path."""

        def close_peer_if_open(peer: _ServerPeer) -> None:
            if peer._socket.fileno() in self._peers:
                self._close_pair(peer, reason='server-closed')

        cleanups = (
            *(partial(close_peer_if_open, peer) for peer in tuple(self._peers.values())),
            *(
                resource.close
                for resource in (self._selector, self._listener)
                if resource is not None
            ),
        )
        first_error: BaseException | None = None
        try:
            close_callables(cleanups)
        except BaseException as error:  # noqa: BLE001
            first_error = error
        self._selector = None
        self._listener = None
        try:
            close_callables(
                (self._unlink_owned_socket,),
                primary_error=first_error,
                failure_note_prefix='additional cleanup failure',
            )
        finally:
            self._bound_identity = None
        if first_error is not None:
            raise first_error

    @property
    def connected(self) -> bool:
        """Return whether the source event listener is open.

        Returns:
            `True` while the private endpoint is accepting peers.
        """
        return self._listener is not None

    @property
    def _connected_groups(self) -> tuple[str, ...]:
        """Return authenticated groups in stable order."""
        return tuple(
            sorted(
                {
                    peer._group
                    for peer in self._peers.values()
                    if peer._group is not None
                    and peer._role == 'event'
                    and peer._connection_id is not None
                    and self._connection_peer(
                        peer._group,
                        peer._connection_id,
                        'coordination',
                    )
                    is not None
                }
            )
        )

    def connected_group_statuses(self) -> tuple[EventConnectionStatus, ...]:
        """Return fresh paired-group status using one per-call peer index.

        Returns:
            Statuses in stable group order, including pairs flushing a detach
            as disconnected. Unpaired roles are omitted.
        """
        peers = {
            (peer._group, peer._connection_id, peer._role): peer
            for peer in self._peers.values()
            if peer._group is not None
            and peer._connection_id is not None
            and peer._role is not None
        }
        statuses: dict[str, EventConnectionStatus] = {}
        for (group, connection_id, role), peer in peers.items():
            if role != 'event':
                continue
            coordination = peers.get((group, connection_id, 'coordination'))
            if coordination is not None:
                statuses[group] = self._status_from_peer(
                    group,
                    connection_id,
                    peer if coordination._close_after_flush is None else None,
                )
        return tuple(statuses[group] for group in sorted(statuses))

    def dispatch_join_requests(self, source: _JoinSource) -> None:
        """Execute queued join work and queue replies on the source thread.

        `poll` accepts and queues coordination requests. This method reports
        queued channel loss, executes the pending source operations, and queues
        their replies without taking ownership of `source`.

        Args:
            source: Source-thread-owned join and channel-loss authority.
        """
        while self._lost_groups:
            source.event_channel_lost(
                self._lost_groups.popleft(),
                reason='event-channel-lost',
            )
        pending_count = len(self._requests)
        for _ in range(pending_count):
            peer, call = self._requests.popleft()
            group = peer._group
            grant = peer._grant
            connection_id = peer._connection_id
            if (
                peer._socket.fileno() < 0
                or peer._role != 'coordination'
                or group is None
                or connection_id is None
                or grant is None
                or grant != self._grants.get(group)
                or peer._active_request_id != call._request_id
            ):
                continue
            if (
                isinstance(call._value, JoinRequest)
                and self._paired_event_peer(group, connection_id) is None
            ):
                self._requests.append((peer, call))
                continue
            error: str | None = None
            result: _JoinResult = None
            try:
                result = self._dispatch_join_call(
                    source,
                    group,
                    connection_id,
                    call,
                )
            except Exception as exception:  # noqa: BLE001
                raise_owner_write_failure(exception)
                error = f'{type(exception).__name__}: {exception}'
            self._queue_join_reply(peer, call, result, error)

    def install_grant(self, grant: EventPeerGrant) -> EventPeerReady:
        """Install or replace one exact dependent authorization.

        Args:
            grant: Supervisor-minted exact source/group/spawn capability.

        Returns:
            Acknowledgement emitted only after old peer bodies are dropped.

        Raises:
            ValueError: The grant names a different source.
        """
        if grant.source != self._source:
            raise ValueError('event grant source conflicts with endpoint')
        previous = self._grants.get(grant.group)
        if previous != grant:
            while True:
                peer = next(
                    (item for item in self._peers.values() if item._group == grant.group),
                    None,
                )
                if peer is None:
                    break
                self._close_pair(peer, reason='grant-replaced')
            self._connection_fences.pop(grant.group, None)
            self._grants[grant.group] = grant
        return EventPeerReady(
            source=grant.source,
            group=grant.group,
            dependent_spawn_instance=grant.dependent_spawn_instance,
        )

    def poll(self, timeout: float = 0.0) -> None:
        """Drive accepts, authentication, EOF detection, and partial writes.

        Args:
            timeout: Maximum selector wait in seconds.

        Raises:
            RuntimeError: The server is not open.
        """
        selector = self._selector
        if selector is None:
            raise RuntimeError('event server is not open')
        for key, mask in selector.select(timeout):
            if key.data is None:
                self._accept()
                continue
            peer = cast('_ServerPeer', key.data)
            if mask & selectors.EVENT_READ:
                self._read_peer(peer)
            if peer._socket.fileno() < 0:
                continue
            if mask & selectors.EVENT_WRITE:
                try:
                    peer._writer._flush(peer._socket)
                except (ConnectionError, OSError):
                    self._close_pair(peer, reason='peer-write-failed')
                    continue
                if not peer._writer._has_pending and peer._close_after_flush is not None:
                    self._close_pair(peer, reason=peer._close_after_flush)
                    continue
                if peer._role == 'coordination' and not peer._writer._pending_count:
                    peer._active_request_id = None
            self._modify_peer(peer)

    @property
    def socket_buffers(self) -> SocketBufferSizes:
        """Return server socket buffer evidence in bytes.

        Returns:
            Configured requested bytes with zero effective sizes before the
            first `start` call. A start attempt replaces them with listener
            values; each accept replaces them with the most recently accepted
            socket's values. `close` retains the last recorded sample.
        """
        return self._socket_buffers

    def start(self) -> None:
        """Bind and listen on the exact private mode-0600 endpoint.

        Calling again while open is a no-op. An existing path is removed only
        after verifying that it is a stale same-user mode-0600 socket.

        Raises:
            SecurityError: An existing endpoint is unsafe, live, or replaced
                during inspection, or its liveness cannot be established.
        """
        if self._listener is not None:
            return
        remove_stale_socket(self._path)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self._socket_buffers = _request_socket_buffers(
                listener,
                self._max_frame_bytes,
            )
            with _EVENT_SOCKET_UMASK_LOCK:
                prior_umask = os.umask(0o177)
                try:
                    listener.bind(os.fspath(self._path))
                finally:
                    os.umask(prior_umask)
            self._path.chmod(FILE_MODE)
            metadata = self._path.lstat()
            self._bound_identity = (metadata.st_dev, metadata.st_ino)
            listener.listen()
            listener.settimeout(0.0)
            selector = selectors.DefaultSelector()
            selector.register(listener, selectors.EVENT_READ, None)
        except BaseException:
            listener.close()
            self.close()
            raise
        self._listener = listener
        self._selector = selector


def _array(value: object, *, context: str) -> list[object]:
    """Return one validated JSON array."""
    if not isinstance(value, list):
        raise _FrameError(f'{context} must be an array')
    return cast('list[object]', value)


def _attempt_payload(attempt: JoinAttemptIdentity) -> dict[str, object]:
    """Encode one complete lane-minted join attempt."""
    return {
        'attempt_token': attempt.attempt_token,
        'group': attempt.group,
        'group_generation': attempt.group_generation,
        'join_kind': attempt.join_kind,
        'site': attempt.site,
        'source': attempt.source,
    }


def _bootstrap_reply_payload(
    result: _JoinResult,
) -> tuple[str, dict[str, object]] | None:
    """Encode one bootstrap reply variant, if present."""
    if isinstance(result, GroupBootstrapObservation):
        return (
            'bootstrap-observation',
            {
                'captured_frontier': result.captured_frontier,
                'group': result.group,
                'group_generation': result.group_generation,
                'source': result.source,
                'source_generation': result.source_generation,
                'source_successful_clock': result.source_successful_clock,
                'stream_incarnation': result.stream_incarnation,
            },
        )
    if isinstance(result, GroupBootstrapPending):
        return (
            'bootstrap-pending',
            {
                'group': result.group,
                'group_generation': result.group_generation,
                'retry_after_secs': result.retry_after_secs,
                'source': result.source,
            },
        )
    return None


def _bootstrap_request_payload(
    request: GroupBootstrapRequest,
) -> dict[str, object]:
    """Encode one group bootstrap observation request."""
    return {
        'group': request.group,
        'group_generation': request.group_generation,
        'source': request.source,
        'start_mode': request.start_mode,
    }


def _conditional_park_directive_payload(
    directive: ConditionalParkDirective,
) -> dict[str, object]:
    """Return one complete group recovery directive payload."""
    return {
        'event_id': directive.event_id,
        'evidence_id': directive.evidence_id,
        'failed_spawn_instance': directive.failed_spawn_instance,
        'group': directive.group,
        'group_generation': directive.group_generation,
        'group_revision': directive.group_revision,
        'observed_at_wall_clock': directive.observed_at_wall_clock,
        'site': directive.site,
    }


def _contains_secret_key(key: str) -> bool:
    """Return whether one normalized field name implies secret material."""
    normalized = key.casefold().replace('-', '_')
    return any(
        normalized == term or normalized.endswith(f'_{term}') or normalized.startswith(f'{term}_')
        for term in _SECRET_KEY_TERMS
    )


def _decode_json(value: bytes, *, context: str) -> dict[str, object]:
    """Decode one strict duplicate-free JSON object."""

    def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        decoded: dict[str, object] = {}
        for key, item in pairs:
            if key in decoded:
                raise _FrameError(f'{context} has a duplicate JSON key')
            decoded[key] = item
        return decoded

    def reject_constant(constant: str) -> object:
        raise _FrameError(f'{context} has a non-finite number: {constant}')

    try:
        decoded = json.loads(
            value.decode('utf-8', errors='strict'),
            object_pairs_hook=object_pairs,
            parse_constant=reject_constant,
        )
    except _FrameError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise _FrameError(f'{context} is invalid: {error}') from error
    if not isinstance(decoded, dict):
        raise _FrameError(f'{context} must be an object')
    return cast('dict[str, object]', decoded)


def _detach_frame(reason: str) -> _OutboundFrame:
    """Encode one body-free peer detach."""
    metadata = {'reason': reason}
    header = encode_event_header('detach', metadata)
    return _OutboundFrame(
        _header=header,
        _payload=b'',
        _charge_bytes=event_frame_bytes('detach', metadata),
    )


def _encode_fanout_message(message: FanoutMessage) -> _OutboundFrame:
    """Frame one immutable fanout message without reserializing event bodies."""
    kind, metadata, payload = message.wire_parts()
    header = encode_event_header(kind, metadata)
    return _OutboundFrame(
        _header=header,
        _payload=payload,
        _charge_bytes=message.byte_count,
    )


def _encode_json(value: Mapping[str, object]) -> bytes:
    """Encode one canonical strict JSON object."""
    try:
        return canonical_bytes(value)
    except (TypeError, UnicodeEncodeError, ValueError) as error:
        raise _FrameError(f'internal message cannot be encoded: {error}') from error


def _auth_body(authentication: _EventAuthentication) -> bytes:
    """Encode one event peer authentication body."""
    return _encode_json(
        {
            'capability': authentication._capability,
            'connection_id': authentication._connection_id,
            'group': authentication._group,
            'role': authentication._role,
            'schema': _AUTH_SCHEMA,
            'source': authentication._source,
            'spawn_instance': authentication._spawn_instance,
            'version': _EVENT_AUTH_PROTOCOL_VERSION,
        }
    )


def _event_control_frame(
    kind: str,
    metadata: Mapping[str, object],
) -> _OutboundFrame:
    """Encode one body-free event-channel control frame."""
    header = encode_event_header(kind, metadata)
    return _OutboundFrame(
        _header=header,
        _payload=b'',
        _charge_bytes=event_frame_bytes(kind, metadata),
    )


def _encode_join_call(call: _JoinCall) -> _OutboundFrame:
    """Encode one closed request-correlated lane-to-source call."""
    metadata: dict[str, object] = {'request_id': call._request_id}
    value = call._value
    if isinstance(value, GroupBootstrapRequest):
        metadata.update(_bootstrap_request_payload(value))
        kind = 'group-bootstrap-observe'
    elif isinstance(value, JoinRequest):
        metadata['attempt'] = _attempt_payload(value.attempt)
        kind = 'join-request'
    elif isinstance(value, JoinConfirm):
        metadata['identity'] = value.identity.wire_payload()
        kind = 'join-confirm'
    elif isinstance(value, JoinFinalize):
        metadata['identity'] = value.identity.wire_payload()
        kind = 'join-finalize'
    elif isinstance(value, JoinAbort):
        metadata['identity'] = value.identity.wire_payload()
        kind = 'join-abort'
    else:
        control = value
        metadata['control_kind'] = (
            'capacity' if isinstance(control, StrictCapacityControl) else 'ordering'
        )
        control_metadata = strict_control_metadata(control)
        metadata['identity'] = control_metadata.pop('identity')
        metadata.update(control_metadata)
        kind = 'join-acknowledge'
    return _event_control_frame(kind, metadata)


def _encode_join_reply(reply: _JoinReply) -> _OutboundFrame:
    """Encode one closed request-correlated source reply."""
    result = reply._result
    payload: dict[str, object] | None
    bootstrap_reply = _bootstrap_reply_payload(result)
    if bootstrap_reply is not None:
        result_kind, payload = bootstrap_reply
    elif isinstance(result, JoinRetry):
        result_kind = 'retry'
        payload = {
            'attempt': _attempt_payload(result.attempt),
            'headroom_bytes': result.headroom_bytes,
            'headroom_events': result.headroom_events,
            'retry_after_secs': result.retry_after_secs,
        }
    elif isinstance(result, JoinTarget):
        result_kind = 'target'
        payload = {
            'identity': result.identity.wire_payload(),
            'source_successful_clock': result.source_successful_clock,
            'stream_incarnation': result.stream_incarnation,
        }
    elif isinstance(result, Attached):
        result_kind = 'attached'
        payload = result.identity.wire_payload()
    elif isinstance(result, JoinFinalized):
        result_kind = 'finalized'
        payload = result.identity.wire_payload()
    else:
        result_kind = 'none'
        payload = None
    return _event_control_frame(
        'join-reply',
        {
            'error': reply._error,
            'request_id': reply._request_id,
            'result': payload,
            'result_kind': result_kind,
        },
    )


def _exact_fields(
    value: Mapping[str, object],
    fields: Collection[str],
    *,
    context: str,
) -> None:
    """Require one exact closed field set."""
    if set(value) != set(fields):
        raise _FrameError(f'{context} fields conflict')


def _decode_event_body(body: bytes) -> tuple[str, dict[str, object], bytes]:
    """Decode event metadata and payload from an already unprefixed body."""
    if len(body) < _LENGTH_BYTES:
        raise _FrameError('event frame header length is truncated')
    (header_length,) = struct.unpack('!I', body[:_LENGTH_BYTES])
    if header_length <= 0 or header_length > len(body) - _LENGTH_BYTES:
        raise _FrameError('event frame header length conflicts')
    header_end = _LENGTH_BYTES + header_length
    header = _decode_json(body[_LENGTH_BYTES:header_end], context='event frame')
    payload = body[header_end:]
    _exact_fields(
        header,
        {'kind', 'metadata', 'schema', 'version'},
        context='event frame',
    )
    if header['schema'] != EVENT_SCHEMA:
        raise _FrameError('event frame schema conflicts')
    if header['version'] != EVENT_VERSION:
        raise _FrameError('event frame version conflicts')
    kind = header['kind']
    if not isinstance(kind, str) or kind not in _EVENT_KINDS:
        raise _FrameError('event frame kind conflicts')
    metadata = header['metadata']
    if not isinstance(metadata, dict):
        raise _FrameError('event frame metadata must be an object')
    return kind, cast('dict[str, object]', metadata), payload


def _finite_float(value: object, *, context: str) -> float:
    """Decode one exact finite JSON integer or float with protocol context."""
    if type(value) not in {float, int}:
        raise _FrameError(context)
    try:
        normalized = float(cast('float | int', value))
    except OverflowError as error:
        raise _FrameError(context) from error
    if not isfinite(normalized):
        raise _FrameError(context)
    return normalized


def _frame_length(value: bytes | bytearray, *, max_frame_bytes: int) -> int | None:
    """Read and validate a complete prefix, or await remaining prefix bytes."""
    if len(value) < _LENGTH_BYTES:
        return None
    (length,) = struct.unpack('!I', value[:_LENGTH_BYTES])
    if length <= 0:
        raise _FrameError('frame length must be positive')
    if length > max_frame_bytes:
        raise _FrameError('frame body length exceeds configured maximum')
    return length


def _freeze_health_payload(value: Mapping[str, object]) -> Mapping[str, object]:
    """Copy and recursively freeze a secret-free health payload."""

    def _freeze(item: object, *, key: str | None = None) -> object:
        if key is not None and _contains_secret_key(key):
            raise ValueError(f'health contains secret-looking field: {key}')
        if item is None or isinstance(item, bool | int | float):
            return item
        if isinstance(item, str):
            if looks_secret(item):
                raise ValueError('health contains secret-looking text')
            return item
        if isinstance(item, Mapping):
            mapped = cast('Mapping[object, object]', item)
            if not all(isinstance(name, str) for name in mapped):
                raise TypeError('health mappings must use string keys')
            string_mapping = cast('Mapping[str, object]', mapped)
            return MappingProxyType(
                {name: _freeze(child, key=name) for name, child in string_mapping.items()}
            )
        if isinstance(item, tuple | list):
            return tuple(_freeze(child) for child in cast('Collection[object]', item))
        raise TypeError(f'unsupported health value: {type(item).__name__}')

    return cast('Mapping[str, object]', _freeze(value))


def _grant_payload(grant: EventPeerGrant) -> dict[str, object]:
    """Return one event grant as a closed JSON-native payload."""
    return {
        'capability': grant.capability,
        'dependent_spawn_instance': grant.dependent_spawn_instance,
        'group': grant.group,
        'source': grant.source,
    }


def _group_assignment_payload(
    assignment: DeliveryGroupAssignment,
) -> dict[str, object]:
    """Return one lane assignment as a closed JSON-native payload."""
    return {
        'assignment_kind': 'delivery',
        'conditional_park': (
            None
            if assignment.conditional_park is None
            else _conditional_park_directive_payload(assignment.conditional_park)
        ),
        'group': assignment.group,
        'group_generation': assignment.group_generation,
        'source': assignment.source,
        'start_mode': assignment.start_mode,
    }


def _group_recovery_ready_payload(
    ready: GroupRecoveryReady,
) -> dict[str, object]:
    """Return one exact group recovery acknowledgement payload."""
    return {
        'evidence_id': ready.evidence_id,
        'group': ready.group,
        'group_generation': ready.group_generation,
        'outcome': ready.outcome,
        'resulting_revision': ready.resulting_revision,
        'site': ready.site,
        'spawn_instance': ready.spawn_instance,
    }


def _health_bool(fields: Mapping[str, object], name: str) -> bool:
    """Require an exact wire boolean without truthiness conversion."""
    value = fields[name]
    if type(value) is not bool:
        raise _FrameError(f'health {name} must be boolean')
    return value


def _health_fields(
    value: object,
    expected: Collection[str],
    *,
    context: str,
) -> Mapping[str, object]:
    """Require one exact secret-safe health object before decoding its fields."""
    if not isinstance(value, dict):
        raise _FrameError(f'{context} must be an object')
    mapping = cast('dict[str, object]', value)
    _exact_fields(mapping, expected, context=context)
    return _freeze_health_payload(mapping)


def _health_int(fields: Mapping[str, object], name: str) -> int:
    """Require an integer health field excluding booleans."""
    value = fields[name]
    if type(value) is not int:
        raise _FrameError(f'health {name} must be an integer')
    return value


def _health_number(fields: Mapping[str, object], name: str) -> float:
    """Require a numeric health field without changing its wire value."""
    value = fields[name]
    if type(value) is not int and type(value) is not float:
        raise _FrameError(f'health {name} must be a number')
    return value


def _health_optional_int(fields: Mapping[str, object], name: str) -> int | None:
    """Require an optional non-boolean integer health field."""
    return None if fields[name] is None else _health_int(fields, name)


def _health_optional_number(fields: Mapping[str, object], name: str) -> float | None:
    """Require an optional numeric health field."""
    return None if fields[name] is None else _health_number(fields, name)


def _health_payload_wire(value: Mapping[str, object]) -> dict[str, object]:
    """Copy one frozen health payload into JSON-native containers."""

    def _copy(item: object) -> object:
        if isinstance(item, Mapping):
            mapping = cast('Mapping[str, object]', item)
            return {key: _copy(child) for key, child in mapping.items()}
        if isinstance(item, tuple):
            sequence = cast('tuple[object, ...]', item)
            return [_copy(child) for child in sequence]
        return item

    return cast('dict[str, object]', _copy(value))


def _health_text(fields: Mapping[str, object], name: str) -> str:
    """Require a text health field without changing its content."""
    value = fields[name]
    if not isinstance(value, str):
        raise _FrameError(f'health {name} must be text')
    return value


def _health_enum(fields: Mapping[str, object], name: str, choices: tuple[str, ...]) -> str:
    """Require one existing health literal after narrowing its string type."""
    value = _health_text(fields, name)
    if value not in choices:
        raise _FrameError(f'health {name} conflicts')
    return value


def _health_optional_text(fields: Mapping[str, object], name: str) -> str | None:
    """Require an optional text health field."""
    return None if fields[name] is None else _health_text(fields, name)


def _decode_event_socket_health(value: object) -> EventSocketHealth:
    """Decode exact socket field types before applying record invariants."""
    fields = _health_fields(
        value, EventSocketHealth.__dataclass_fields__, context='event socket health'
    )
    return EventSocketHealth(
        source=_health_text(fields, 'source'),
        group=_health_optional_text(fields, 'group'),
        role=cast(
            "Literal['server', 'client']", _health_enum(fields, 'role', ('server', 'client'))
        ),
        connected=_health_bool(fields, 'connected'),
        requested=_health_int(fields, 'requested'),
        send=_health_int(fields, 'send'),
        receive=_health_int(fields, 'receive'),
        queue_event_count=_health_int(fields, 'queue_event_count'),
        queue_byte_count=_health_int(fields, 'queue_byte_count'),
        reason=_health_optional_text(fields, 'reason'),
    )


def _decode_watchdog_health(value: object) -> WatchdogHealth:
    """Decode exact watchdog field types before applying record invariants."""
    fields = _health_fields(value, WatchdogHealth.__dataclass_fields__, context='watchdog health')
    return WatchdogHealth(
        kind=cast('WatchdogKind', _health_enum(fields, 'kind', WATCHDOG_KINDS)),
        site=_health_text(fields, 'site'),
        source=_health_optional_text(fields, 'source'),
        group=_health_optional_text(fields, 'group'),
        worker_instance=_health_text(fields, 'worker_instance'),
        phase=cast(
            'WatchdogPhase',
            _health_enum(fields, 'phase', WATCHDOG_PHASES),
        ),
        heartbeat_at=_health_number(fields, 'heartbeat_at'),
        deadline_at=_health_optional_number(fields, 'deadline_at'),
        event_id=_health_optional_int(fields, 'event_id'),
        plugin_id=_health_optional_text(fields, 'plugin_id'),
        callback_id=_health_optional_text(fields, 'callback_id'),
        delivery_attempt=_health_optional_int(fields, 'delivery_attempt'),
        operator_command=_health_optional_text(fields, 'operator_command'),
    )


def _health_tuple(value: object) -> tuple[object, ...]:
    """Require an array already frozen at the health-object boundary."""
    if not isinstance(value, tuple):
        raise _FrameError('health collection must be an array')
    return cast('tuple[object, ...]', value)


def _health_ints(value: object) -> tuple[int, ...]:
    """Require integer members in one frozen health array."""
    items = _health_tuple(value)
    if not all(type(item) is int for item in items):
        raise _FrameError('health collection members must be integers')
    return cast('tuple[int, ...]', items)


def _decode_source_health(value: object) -> SourceHealth:
    """Decode exact source field types before applying record invariants."""
    fields = _health_fields(value, SourceHealth.__dataclass_fields__, context='source health')
    retention: list[tuple[str, int, int, int]] = []
    for item in _health_tuple(fields['strict_retention']):
        row = _health_tuple(item)
        if len(row) != _STRICT_RETENTION_LENGTH or not isinstance(row[0], str):
            raise _FrameError('source health strict retention row conflicts')
        counts = _health_ints(row[1:])
        retention.append((row[0], counts[0], counts[1], counts[2]))
    return SourceHealth(
        site=_health_text(fields, 'site'),
        source=_health_text(fields, 'source'),
        source_generation=_health_text(fields, 'source_generation'),
        status=cast(
            'SourceRunStatus',
            _health_enum(
                fields,
                'status',
                SOURCE_RUN_STATUSES,
            ),
        ),
        frontier=_health_int(fields, 'frontier'),
        successful_clock=_health_number(fields, 'successful_clock'),
        retry_attempt=_health_int(fields, 'retry_attempt'),
        next_retry_secs=_health_optional_number(fields, 'next_retry_secs'),
        open_range_count=_health_int(fields, 'open_range_count'),
        late_id_count=_health_int(fields, 'late_id_count'),
        last_error=_health_optional_text(fields, 'last_error'),
        worker_instance=_health_text(fields, 'worker_instance'),
        initialized=_health_bool(fields, 'initialized'),
        pending_obligation_count=_health_int(fields, 'pending_obligation_count'),
        orphan_marker_count=_health_int(fields, 'orphan_marker_count'),
        strict_buffer_event_count=_health_int(fields, 'strict_buffer_event_count'),
        strict_buffer_byte_count=_health_int(fields, 'strict_buffer_byte_count'),
        strict_retention=tuple(retention),
        checkpoint_state=_health_text(fields, 'checkpoint_state'),
        stream_incarnation=_health_optional_text(fields, 'stream_incarnation'),
        audit_count=_health_int(fields, 'audit_count'),
        database_size_bytes=_health_int(fields, 'database_size_bytes'),
        phase=cast(
            'SourceWatchdogPhase',
            _health_enum(fields, 'phase', SOURCE_WATCHDOG_PHASES),
        ),
        heartbeat_at=_health_number(fields, 'heartbeat_at'),
        heartbeat_age=_health_number(fields, 'heartbeat_age'),
        deadline_at=_health_optional_number(fields, 'deadline_at'),
        overdue_reason=_health_optional_text(fields, 'overdue_reason'),
    )


def _health_strings(value: object) -> tuple[str, ...]:
    """Require string members in one frozen health array."""
    items = _health_tuple(value)
    if not all(isinstance(item, str) for item in items):
        raise _FrameError('health collection members must be text')
    return cast('tuple[str, ...]', items)


def _decode_group_health(value: object) -> GroupHealth:
    """Decode exact group field types before applying record invariants."""
    fields = _health_fields(value, GroupHealth.__dataclass_fields__, context='group health')
    return GroupHealth(
        site=_health_text(fields, 'site'),
        source=_health_text(fields, 'source'),
        group=_health_text(fields, 'group'),
        worker_instance=_health_text(fields, 'worker_instance'),
        execution=cast(
            "Literal['sync', 'async']", _health_enum(fields, 'execution', ('sync', 'async'))
        ),
        ordering=cast(
            "Literal['eager', 'strict']", _health_enum(fields, 'ordering', ('eager', 'strict'))
        ),
        phase=cast(
            'GroupPhase',
            _health_enum(fields, 'phase', GROUP_PHASES),
        ),
        status=_health_text(fields, 'status'),
        cursor=_health_int(fields, 'cursor'),
        current_event_id=_health_optional_int(fields, 'current_event_id'),
        current_plugin_id=_health_optional_text(fields, 'current_plugin_id'),
        current_callback_id=_health_optional_text(fields, 'current_callback_id'),
        delivery_attempt=_health_optional_int(fields, 'delivery_attempt'),
        heartbeat_at=_health_number(fields, 'heartbeat_at'),
        heartbeat_age=_health_number(fields, 'heartbeat_age'),
        queue_event_count=_health_int(fields, 'queue_event_count'),
        queue_byte_count=_health_int(fields, 'queue_byte_count'),
        catchup_frontier=_health_optional_int(fields, 'catchup_frontier'),
        pending_catchup_epoch_count=_health_int(fields, 'pending_catchup_epoch_count'),
        oldest_catchup_epoch_age=_health_optional_number(fields, 'oldest_catchup_epoch_age'),
        join_phase=_health_text(fields, 'join_phase'),
        outstanding_late_obligation_count=_health_int(fields, 'outstanding_late_obligation_count'),
        outstanding_late_id_count=_health_int(fields, 'outstanding_late_id_count'),
        outstanding_late_ids=_health_ints(fields['outstanding_late_ids']),
        outstanding_late_ids_truncated=_health_bool(fields, 'outstanding_late_ids_truncated'),
        parked=_health_bool(fields, 'parked'),
        park_reason=_health_optional_text(fields, 'park_reason'),
        failure_kind=_health_optional_text(fields, 'failure_kind'),
        failure_text=_health_optional_text(fields, 'failure_text'),
        checkpoint_state=_health_text(fields, 'checkpoint_state'),
        conditions=_health_strings(fields['conditions']),
        audit_count=_health_int(fields, 'audit_count'),
        deadline_at=_health_optional_number(fields, 'deadline_at'),
        overdue_reason=_health_optional_text(fields, 'overdue_reason'),
    )


def _decode_worker_health(value: object) -> WorkerHealth:
    """Decode exact worker field types before applying record invariants."""
    fields = _health_fields(value, WorkerHealth.__dataclass_fields__, context='worker health')
    outages: list[tuple[str, str]] = []
    for item in _health_tuple(fields['source_outages']):
        row = _health_strings(item)
        if len(row) != _PAIR_LENGTH:
            raise _FrameError('worker health source outage row conflicts')
        outages.append((row[0], row[1]))
    return WorkerHealth(
        site=_health_text(fields, 'site'),
        worker_id=_health_text(fields, 'worker_id'),
        spawn_instance=_health_text(fields, 'spawn_instance'),
        heartbeat_at=_health_number(fields, 'heartbeat_at'),
        monitor_alive=_health_bool(fields, 'monitor_alive'),
        thread_names=_health_strings(fields['thread_names']),
        stuck_threads=_health_strings(fields['stuck_threads']),
        breaker_open=_health_bool(fields, 'breaker_open'),
        breaker_reason=_health_optional_text(fields, 'breaker_reason'),
        socket_buffer_requested=_health_int(fields, 'socket_buffer_requested'),
        socket_send_buffer=_health_int(fields, 'socket_send_buffer'),
        socket_receive_buffer=_health_int(fields, 'socket_receive_buffer'),
        source_outages=tuple(outages),
        heartbeat_age=_health_number(fields, 'heartbeat_age'),
        watchdog_failure=_health_optional_text(fields, 'watchdog_failure'),
    )


def _decode_health_update(fields: Mapping[str, object]) -> HealthUpdate:
    """Decode one closed health-update payload with concrete record contracts."""
    _exact_fields(
        fields,
        {'event_sockets', 'groups', 'sources', 'watchdogs', 'worker'},
        context='health update',
    )
    try:
        return HealthUpdate(
            worker=_decode_worker_health(fields['worker']),
            watchdogs=tuple(
                _decode_watchdog_health(item)
                for item in _array(fields['watchdogs'], context='watchdog health')
            ),
            sources=tuple(
                _decode_source_health(item)
                for item in _array(fields['sources'], context='source health')
            ),
            groups=tuple(
                _decode_group_health(item)
                for item in _array(fields['groups'], context='group health')
            ),
            event_sockets=tuple(
                _decode_event_socket_health(item)
                for item in _array(fields['event_sockets'], context='event socket health')
            ),
        )
    except (TypeError, ValueError) as error:
        raise _FrameError(f'health update is invalid: {error}') from error


def _positive_int(value: object, *, context: str) -> int:
    """Return one positive non-boolean integer."""
    if type(value) is not int or value <= 0:
        raise _FrameError(f'{context} must be a positive integer')
    return value


def _decode_event_delivery(
    fields: Mapping[str, object],
    payload: bytes,
) -> EventDelivery:
    """Decode one validated immutable event body."""
    _exact_fields(fields, {'event_id'}, context='event metadata')
    event_id = _positive_int(fields['event_id'], context='event id')
    try:
        decode_event(payload)
    except ValueError as error:
        raise _FrameError(f'event body is invalid: {error}') from error
    return EventDelivery(EncodedEvent.from_event_frame(event_id, payload))


def _decode_strict_capacity_fields(
    fields: Mapping[str, object],
    identity: LiveJoinIdentity,
) -> StrictCapacityControl:
    """Decode contained capacity values after the caller validates its envelope."""
    return StrictCapacityControl(
        identity=identity,
        abort_limit=_positive_int(fields['abort_limit'], context='strict capacity abort limit'),
    )


def _decode_strict_ordering_fields(
    fields: Mapping[str, object],
    identity: LiveJoinIdentity,
) -> StrictOrderingControl:
    """Decode contained ordering values after the caller validates its envelope."""
    action = fields['action']
    if not isinstance(action, str) or action not in {'halt', 'skip'}:
        raise _FrameError('strict ordering action conflicts')
    delivery_floor = fields['delivery_floor']
    last_delivered_id = fields['last_delivered_id']
    if type(delivery_floor) is not int or delivery_floor < 0:
        raise _FrameError('strict ordering delivery floor must be nonnegative')
    if type(last_delivered_id) is not int or last_delivered_id < 0:
        raise _FrameError('strict ordering last delivered id must be nonnegative')
    return StrictOrderingControl(
        identity=identity,
        event_id=_positive_int(
            fields['event_id'],
            context='strict ordering event id',
        ),
        last_delivered_id=last_delivered_id,
        delivery_floor=delivery_floor,
        action=cast("Literal['halt', 'skip']", action),
    )


def _decode_join_control(
    fields: Mapping[str, object],
    identity: LiveJoinIdentity,
) -> StrictCapacityControl | StrictOrderingControl:
    """Decode one strict capacity or ordering acknowledgement."""
    control_kind = fields.get('control_kind')
    if control_kind == 'capacity':
        _exact_fields(
            fields,
            {'abort_limit', 'control_kind', 'identity', 'request_id'},
            context='strict capacity acknowledgement',
        )
        return _decode_strict_capacity_fields(fields, identity)
    if control_kind != 'ordering':
        raise _FrameError('strict acknowledgement kind conflicts')
    _exact_fields(
        fields,
        {
            'action',
            'control_kind',
            'delivery_floor',
            'event_id',
            'identity',
            'last_delivered_id',
            'request_id',
        },
        context='strict ordering acknowledgement',
    )
    return _decode_strict_ordering_fields(fields, identity)


def _request_socket_buffers(sock: socket.socket, requested: int) -> SocketBufferSizes:
    """Request bounded buffers and record platform-reported effective values."""
    if requested <= 0:
        raise ValueError('requested socket buffer bytes must be positive')
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, requested)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, requested)
    return SocketBufferSizes(
        requested=requested,
        send=int(sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)),
        receive=int(sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)),
    )


def _source_endpoint_payload(endpoint: SourceEndpointReady) -> dict[str, object]:
    """Return one source endpoint as a closed JSON-native payload."""
    return {
        'path': endpoint.path,
        'source': endpoint.source,
        'source_generation': endpoint.source_generation,
    }


def _source_group_assignment_payload(
    assignment: SourceGroupAssignment,
) -> dict[str, object]:
    """Return one source-group inventory entry as a closed payload."""
    if isinstance(assignment, PendingDispositionSourceGroupAssignment):
        return {
            'assignment_kind': 'pending-disposition',
            'group': assignment.group,
            'orphan_generation': assignment.orphan_generation,
            'reserved_generation': assignment.reserved_generation,
            'source': assignment.source,
        }
    return {
        'assignment_kind': 'active',
        'group': assignment.group,
        'group_generation': assignment.group_generation,
        'source': assignment.source,
        'start_mode': assignment.start_mode,
    }


def _message_payload(message: ControlMessage) -> tuple[str, dict[str, object]]:
    """Return the closed kind and JSON payload for one control message."""
    kind: str
    payload: dict[str, object]
    if isinstance(message, ControlHello):
        kind = 'hello'
        payload = {
            'pid': message.pid,
            'spawn_instance': message.spawn_instance,
            'worker_id': message.worker_id,
        }
    elif isinstance(message, WorkerAssignment):
        kind = 'assignment'
        payload = {
            'event_grants': [_grant_payload(grant) for grant in message.event_grants],
            'group_assignments': [
                _group_assignment_payload(assignment) for assignment in message.group_assignments
            ],
            'source_group_assignments': [
                _source_group_assignment_payload(assignment)
                for assignment in message.source_group_assignments
            ],
            'source_endpoints': [
                _source_endpoint_payload(endpoint) for endpoint in message.source_endpoints
            ],
            'source_generations': [
                [source, generation] for source, generation in message.source_generations
            ],
            'spawn_instance': message.spawn_instance,
            'worker_id': message.worker_id,
        }
    elif isinstance(message, GroupRecoveryReady):
        kind = 'group-recovery-ready'
        payload = _group_recovery_ready_payload(message)
    elif isinstance(message, SourceEndpointReady):
        kind = 'source-endpoint-ready'
        payload = _source_endpoint_payload(message)
    elif isinstance(message, EventPeerGrant):
        kind = 'event-peer-grant'
        payload = _grant_payload(message)
    elif isinstance(message, EventPeerReady):
        kind = 'event-peer-ready'
        payload = {
            'dependent_spawn_instance': message.dependent_spawn_instance,
            'group': message.group,
            'source': message.source,
        }
    elif isinstance(message, StopCommand):
        kind = 'stop'
        payload = {'reason': message.reason}
    elif isinstance(message, HealthUpdate):
        kind = 'health'
        payload = _health_payload_wire(cast('dict[str, object]', asdict(message)))
    elif isinstance(message, OperatorCommand):
        kind = 'operator-command'
        payload = {
            'arguments': _health_payload_wire(message.arguments),
            'command': message.command,
            'identity': {
                'local_uid': message.identity.local_uid,
                'pid': message.identity.pid,
                'username': message.identity.username,
            },
            'request_id': message.request_id,
        }
    else:
        kind = 'command-result'
        payload = {
            'command': message.command,
            'message': message.message,
            'ok': message.ok,
            'request_id': message.request_id,
            'result': _health_payload_wire(message.result),
        }
    return kind, payload


def _string(value: object, *, context: str) -> str:
    """Return one nonempty string."""
    if not isinstance(value, str) or not value:
        raise _FrameError(f'{context} must be a nonempty string')
    return value


def _decode_attempt(payload: Mapping[str, object]) -> JoinAttemptIdentity:
    """Decode one complete lane-minted join attempt."""
    _exact_fields(
        payload,
        {
            'attempt_token',
            'group',
            'group_generation',
            'join_kind',
            'site',
            'source',
        },
        context='join attempt',
    )
    join_kind = payload['join_kind']
    if not isinstance(join_kind, str) or join_kind not in {'eager', 'strict'}:
        raise _FrameError('join attempt kind conflicts')
    generation = payload['group_generation']
    if type(generation) is not int or generation < 0:
        raise _FrameError('join attempt generation must be nonnegative')
    return JoinAttemptIdentity(
        site=_string(payload['site'], context='join attempt site'),
        source=_string(payload['source'], context='join attempt source'),
        group=_string(payload['group'], context='join attempt group'),
        join_kind=cast("Literal['eager', 'strict']", join_kind),
        group_generation=generation,
        attempt_token=_string(
            payload['attempt_token'],
            context='join attempt token',
        ),
    )


def _decode_auth_body(value: bytes) -> _EventAuthentication:
    """Decode one event peer authentication body."""
    payload = _decode_json(value, context='event authentication')
    _exact_fields(
        payload,
        {
            'capability',
            'connection_id',
            'group',
            'role',
            'schema',
            'source',
            'spawn_instance',
            'version',
        },
        context='event authentication',
    )
    if payload['schema'] != _AUTH_SCHEMA:
        raise _IPCAuthenticationError('event protocol schema conflicts')
    if payload['version'] != _EVENT_AUTH_PROTOCOL_VERSION:
        raise _IPCAuthenticationError('event protocol version conflicts')
    role = payload['role']
    if not isinstance(role, str) or role not in {'coordination', 'event'}:
        raise _IPCAuthenticationError('event peer role conflicts')
    return _EventAuthentication(
        _capability=_string(
            payload['capability'],
            context='event authentication capability',
        ),
        _connection_id=_string(
            payload['connection_id'],
            context='event authentication connection identity',
        ),
        _group=_string(payload['group'], context='event authentication group'),
        _role=cast("Literal['coordination', 'event']", role),
        _source=_string(payload['source'], context='event authentication source'),
        _spawn_instance=_string(
            payload['spawn_instance'],
            context='event authentication spawn instance',
        ),
    )


def _decode_bootstrap_observation(
    mapping: Mapping[str, object],
) -> GroupBootstrapObservation:
    """Decode one source observation reply."""
    _exact_fields(
        mapping,
        {
            'captured_frontier',
            'group',
            'group_generation',
            'source',
            'source_generation',
            'source_successful_clock',
            'stream_incarnation',
        },
        context='group bootstrap observation',
    )
    frontier = mapping['captured_frontier']
    generation = mapping['group_generation']
    successful_clock = mapping['source_successful_clock']
    if type(frontier) is not int or frontier < 0:
        raise _FrameError('group bootstrap frontier conflicts')
    if type(generation) is not int or generation < 0:
        raise _FrameError('group bootstrap generation conflicts')
    numeric_clock = _finite_float(
        successful_clock,
        context='group bootstrap successful clock conflicts',
    )
    if numeric_clock < 0:
        raise _FrameError('group bootstrap successful clock conflicts')
    return GroupBootstrapObservation(
        source=_string(mapping['source'], context='group bootstrap source'),
        group=_string(mapping['group'], context='group bootstrap group'),
        group_generation=generation,
        source_generation=_string(
            mapping['source_generation'],
            context='group bootstrap source generation',
        ),
        stream_incarnation=_string(
            mapping['stream_incarnation'],
            context='group bootstrap incarnation',
        ),
        captured_frontier=frontier,
        source_successful_clock=numeric_clock,
    )


def _decode_bootstrap_pending(
    mapping: Mapping[str, object],
) -> GroupBootstrapPending:
    """Decode one exact bounded source-initialization retry."""
    _exact_fields(
        mapping,
        {'group', 'group_generation', 'retry_after_secs', 'source'},
        context='group bootstrap pending',
    )
    generation = mapping['group_generation']
    delay = mapping['retry_after_secs']
    if type(generation) is not int or generation < 0:
        raise _FrameError('group bootstrap pending generation conflicts')
    numeric_delay = _finite_float(
        delay,
        context='group bootstrap pending delay conflicts',
    )
    if not 0 < numeric_delay <= _MAX_BOOTSTRAP_RETRY_SECS:
        raise _FrameError('group bootstrap pending delay conflicts')
    return GroupBootstrapPending(
        source=_string(mapping['source'], context='group bootstrap pending identity'),
        group=_string(mapping['group'], context='group bootstrap pending identity'),
        group_generation=generation,
        retry_after_secs=numeric_delay,
    )


def _decode_bootstrap_request(
    fields: Mapping[str, object],
) -> GroupBootstrapRequest:
    """Decode one group bootstrap observation request."""
    _exact_fields(
        fields,
        {'group', 'group_generation', 'source', 'start_mode'},
        context='group bootstrap observation',
    )
    generation = fields['group_generation']
    mode = fields['start_mode']
    if type(generation) is not int or generation < 0:
        raise _FrameError('group bootstrap generation must be nonnegative')
    if not is_start_mode(mode):
        raise _FrameError('group bootstrap start mode conflicts')
    return GroupBootstrapRequest(
        source=_string(fields['source'], context='group bootstrap source'),
        group=_string(fields['group'], context='group bootstrap group'),
        group_generation=generation,
        start_mode=mode,
    )


def _decode_command_result(fields: Mapping[str, object]) -> CommandResult:
    """Decode one closed command-result payload."""
    _exact_fields(
        fields,
        {'command', 'message', 'ok', 'request_id', 'result'},
        context='command result',
    )
    ok = fields['ok']
    if type(ok) is not bool:
        raise _FrameError('command result status must be a boolean')
    message = fields['message']
    if not isinstance(message, str):
        raise _FrameError('command result message must be a string')
    result = fields['result']
    if not isinstance(result, dict):
        raise _FrameError('command result result must be an object')
    return CommandResult(
        command=_string(fields['command'], context='command result command'),
        ok=ok,
        message=message,
        request_id=_string(fields['request_id'], context='command result request id'),
        result=cast('dict[str, object]', result),
    )


def _decode_conditional_park_directive(
    value: object,
) -> ConditionalParkDirective | None:
    """Decode one optional closed group recovery directive."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise _FrameError('conditional park directive must be an object or null')
    fields = cast('dict[str, object]', value)
    _exact_fields(
        fields,
        {
            'event_id',
            'evidence_id',
            'failed_spawn_instance',
            'group',
            'group_generation',
            'group_revision',
            'observed_at_wall_clock',
            'site',
        },
        context='conditional park directive',
    )

    try:
        return ConditionalParkDirective(
            evidence_id=_string(fields['evidence_id'], context='conditional park evidence id'),
            site=_string(fields['site'], context='conditional park site'),
            group=_string(fields['group'], context='conditional park group'),
            group_generation=cast('int', fields['group_generation']),
            failed_spawn_instance=_string(
                fields['failed_spawn_instance'], context='conditional park failed spawn'
            ),
            event_id=cast('int', fields['event_id']),
            group_revision=cast('int', fields['group_revision']),
            observed_at_wall_clock=_finite_float(
                fields['observed_at_wall_clock'],
                context='conditional park directive observation time is invalid',
            ),
        )
    except ValueError as error:
        raise _FrameError(str(error)) from error


def _decode_event_peer_grant(fields: Mapping[str, object]) -> EventPeerGrant:
    """Decode one exact event peer grant."""
    _exact_fields(
        fields,
        {'capability', 'dependent_spawn_instance', 'group', 'source'},
        context='event peer grant',
    )
    return EventPeerGrant(
        source=_string(fields['source'], context='event peer grant source'),
        group=_string(fields['group'], context='event peer grant group'),
        dependent_spawn_instance=_string(
            fields['dependent_spawn_instance'],
            context='event peer grant dependent spawn',
        ),
        capability=_string(
            fields['capability'],
            context='event peer grant capability',
        ),
    )


def _decode_event_grants(value: object) -> tuple[EventPeerGrant, ...]:
    """Decode one worker assignment event-grant array."""
    grants: list[EventPeerGrant] = []
    for item in _array(value, context='worker assignment event grants'):
        if not isinstance(item, dict):
            raise _FrameError('worker assignment grant must be an object')
        grants.append(_decode_event_peer_grant(cast('dict[str, object]', item)))
    return tuple(grants)


def _decode_event_peer_ready(fields: Mapping[str, object]) -> EventPeerReady:
    """Decode one exact event grant acknowledgement."""
    _exact_fields(
        fields,
        {'dependent_spawn_instance', 'group', 'source'},
        context='event peer ready',
    )
    return EventPeerReady(
        source=_string(fields['source'], context='event peer ready source'),
        group=_string(fields['group'], context='event peer ready group'),
        dependent_spawn_instance=_string(
            fields['dependent_spawn_instance'],
            context='event peer ready dependent spawn',
        ),
    )


def _decode_group_assignment(value: object) -> DeliveryGroupAssignment:
    """Decode one closed worker-owned lane assignment."""
    if not isinstance(value, dict):
        raise _FrameError('worker group assignment must be an object')
    fields = cast('dict[str, object]', value)
    kind = fields.get('assignment_kind')
    if kind != 'delivery':
        raise _FrameError('worker group assignment kind conflicts')
    _exact_fields(
        fields,
        {
            'assignment_kind',
            'conditional_park',
            'group',
            'group_generation',
            'source',
            'start_mode',
        },
        context='worker group assignment',
    )
    generation = fields['group_generation']
    if type(generation) is not int or generation < 0:
        raise _FrameError('worker group assignment generation must be nonnegative')
    mode = _string(fields['start_mode'], context='worker group start mode')
    if not is_start_mode(mode):
        raise _FrameError('worker group assignment start mode is invalid')
    return DeliveryGroupAssignment(
        source=_string(fields['source'], context='worker group source'),
        group=_string(fields['group'], context='worker group name'),
        group_generation=generation,
        start_mode=mode,
        conditional_park=_decode_conditional_park_directive(fields['conditional_park']),
    )


def _decode_group_recovery_ready(fields: Mapping[str, object]) -> GroupRecoveryReady:
    """Decode one exact worker group-recovery acknowledgement."""
    _exact_fields(
        fields,
        {
            'evidence_id',
            'group',
            'group_generation',
            'outcome',
            'resulting_revision',
            'site',
            'spawn_instance',
        },
        context='group recovery ready',
    )

    def _recovery_text(field: str, context: str) -> str:
        text = _string(fields[field], context=context)
        if looks_secret(text):
            raise _FrameError(f'{context} contains secret-looking text')
        return text

    generation = fields['group_generation']
    revision = fields['resulting_revision']
    outcome = _string(fields['outcome'], context='group recovery ready outcome')
    if type(generation) is not int or generation < 0:
        raise _FrameError('group recovery ready generation must be nonnegative')
    if type(revision) is not int or revision < 0:
        raise _FrameError('group recovery ready revision must be nonnegative')
    if outcome not in {'parked', 'stale'}:
        raise _FrameError('group recovery ready outcome is invalid')
    return GroupRecoveryReady(
        spawn_instance=_recovery_text(
            'spawn_instance',
            'group recovery ready spawn',
        ),
        evidence_id=_recovery_text(
            'evidence_id',
            'group recovery ready evidence id',
        ),
        site=_recovery_text('site', 'group recovery ready site'),
        group=_recovery_text('group', 'group recovery ready group'),
        group_generation=generation,
        outcome=cast('ConditionalParkOutcome', outcome),
        resulting_revision=revision,
    )


def _decode_hello(fields: Mapping[str, object]) -> ControlHello:
    """Decode one closed worker-hello payload."""
    _exact_fields(
        fields,
        {'pid', 'spawn_instance', 'worker_id'},
        context='worker hello',
    )
    return ControlHello(
        spawn_instance=_string(
            fields['spawn_instance'],
            context='worker hello spawn instance',
        ),
        worker_id=_string(fields['worker_id'], context='worker hello id'),
        pid=_positive_int(fields['pid'], context='worker hello pid'),
    )


def _decode_join_request(fields: Mapping[str, object], request_id: int) -> _JoinCall:
    """Decode one initial join request call."""
    _exact_fields(fields, {'attempt', 'request_id'}, context='join request')
    attempt = fields['attempt']
    if not isinstance(attempt, dict):
        raise _FrameError('join request attempt must be an object')
    return _JoinCall(
        request_id,
        JoinRequest(_decode_attempt(cast('dict[str, object]', attempt))),
    )


def _decode_join_retry(mapping: Mapping[str, object]) -> JoinRetry:
    """Decode one bounded join retry reply."""
    _exact_fields(
        mapping,
        {
            'attempt',
            'headroom_bytes',
            'headroom_events',
            'retry_after_secs',
        },
        context='join retry',
    )
    attempt = mapping['attempt']
    if not isinstance(attempt, dict):
        raise _FrameError('join retry attempt must be an object')
    identity = _decode_attempt(cast('dict[str, object]', attempt))
    try:
        retry = JoinRetry(
            attempt=identity,
            retry_after_secs=cast('float', mapping['retry_after_secs']),
            headroom_events=cast('int', mapping['headroom_events']),
            headroom_bytes=cast('int', mapping['headroom_bytes']),
        )
    except ValueError as exc:
        raise _FrameError('join retry bounds conflict') from exc
    return replace(retry, retry_after_secs=float(retry.retry_after_secs))


def _decode_operator_command(fields: Mapping[str, object]) -> OperatorCommand:
    """Decode one trusted request-correlated operator command."""
    _exact_fields(
        fields,
        {'arguments', 'command', 'identity', 'request_id'},
        context='operator command',
    )
    arguments = fields['arguments']
    identity = fields['identity']
    if not isinstance(arguments, dict):
        raise _FrameError('operator command arguments must be an object')
    if not isinstance(identity, dict):
        raise _FrameError('operator command identity must be an object')
    identity_fields = cast('dict[str, object]', identity)
    _exact_fields(
        identity_fields,
        {'local_uid', 'pid', 'username'},
        context='operator command identity',
    )
    local_uid = identity_fields['local_uid']
    pid = identity_fields['pid']
    if type(local_uid) is not int or local_uid < 0:
        raise _FrameError('operator command user id must be nonnegative')
    if type(pid) is not int or pid <= 0:
        raise _FrameError('operator command process id must be positive')
    return OperatorCommand(
        request_id=_string(fields['request_id'], context='operator command request id'),
        command=_string(fields['command'], context='operator command'),
        arguments=cast('dict[str, object]', arguments),
        identity=AuditIdentity(
            local_uid=local_uid,
            username=_string(
                identity_fields['username'],
                context='operator command username',
            ),
            pid=pid,
        ),
    )


def _decode_source_endpoint(fields: Mapping[str, object]) -> SourceEndpointReady:
    """Decode one source endpoint readiness value."""
    _exact_fields(
        fields,
        {'path', 'source', 'source_generation'},
        context='source endpoint ready',
    )
    return SourceEndpointReady(
        source=_string(fields['source'], context='source endpoint source'),
        source_generation=_string(
            fields['source_generation'],
            context='source endpoint generation',
        ),
        path=_string(fields['path'], context='source endpoint path'),
    )


def _decode_source_endpoints(value: object) -> tuple[SourceEndpointReady, ...]:
    """Decode one worker assignment source-endpoint array."""
    endpoints: list[SourceEndpointReady] = []
    for item in _array(value, context='worker assignment source endpoints'):
        if not isinstance(item, dict):
            raise _FrameError('worker assignment endpoint must be an object')
        endpoints.append(_decode_source_endpoint(cast('dict[str, object]', item)))
    return tuple(endpoints)


def _decode_source_group_assignment(value: object) -> SourceGroupAssignment:
    """Decode one closed source-group inventory assignment."""
    if not isinstance(value, dict):
        raise _FrameError('source group assignment must be an object')
    fields = cast('dict[str, object]', value)
    kind = fields.get('assignment_kind')
    if kind == 'pending-disposition':
        _exact_fields(
            fields,
            {
                'assignment_kind',
                'group',
                'orphan_generation',
                'reserved_generation',
                'source',
            },
            context='source group assignment',
        )
        orphan_generation = fields['orphan_generation']
        reserved_generation = fields['reserved_generation']
        if (
            type(orphan_generation) is not int
            or orphan_generation < 0
            or type(reserved_generation) is not int
            or reserved_generation <= orphan_generation
        ):
            raise _FrameError('pending source generation fence is invalid')
        return PendingDispositionSourceGroupAssignment(
            source=_string(fields['source'], context='source group source'),
            group=_string(fields['group'], context='source group name'),
            orphan_generation=orphan_generation,
            reserved_generation=reserved_generation,
        )
    if kind != 'active':
        raise _FrameError('source group assignment kind conflicts')
    _exact_fields(
        fields,
        {
            'assignment_kind',
            'group',
            'group_generation',
            'source',
            'start_mode',
        },
        context='source group assignment',
    )
    generation = fields['group_generation']
    if type(generation) is not int or generation < 0:
        raise _FrameError('source group assignment generation must be nonnegative')
    mode = _string(fields['start_mode'], context='source group start mode')
    if not is_start_mode(mode):
        raise _FrameError('source group assignment start mode is invalid')
    return ActiveSourceGroupAssignment(
        source=_string(fields['source'], context='source group source'),
        group=_string(fields['group'], context='source group name'),
        group_generation=generation,
        start_mode=mode,
    )


def _decode_stop(fields: Mapping[str, object]) -> StopCommand:
    """Decode one closed stop-command payload."""
    _exact_fields(fields, {'reason'}, context='stop command')
    return StopCommand(reason=_string(fields['reason'], context='stop reason'))


def _decode_string_pairs(
    value: object,
    *,
    context: str,
) -> tuple[tuple[str, str], ...]:
    """Decode one JSON array of text pairs."""
    decoded: list[tuple[str, str]] = []
    for item in _array(value, context=context):
        if not isinstance(item, list):
            raise _FrameError(f'{context} entries must be pairs')
        pair = cast('list[object]', item)
        if len(pair) != _PAIR_LENGTH:
            raise _FrameError(f'{context} entries must be pairs')
        name, child = pair
        decoded.append(
            (
                _string(name, context=f'{context} name'),
                _string(child, context=f'{context} generation'),
            )
        )
    return tuple(decoded)


def _decode_worker_assignment(fields: Mapping[str, object]) -> WorkerAssignment:
    """Decode one closed immutable worker assignment."""
    _exact_fields(
        fields,
        {
            'event_grants',
            'group_assignments',
            'source_endpoints',
            'source_generations',
            'source_group_assignments',
            'spawn_instance',
            'worker_id',
        },
        context='worker assignment',
    )
    group_assignments = tuple(
        _decode_group_assignment(item)
        for item in _array(
            fields['group_assignments'],
            context='worker group assignments',
        )
    )
    source_group_assignments = tuple(
        _decode_source_group_assignment(item)
        for item in _array(
            fields['source_group_assignments'],
            context='worker source group assignments',
        )
    )
    source_pairs = _decode_string_pairs(
        fields['source_generations'],
        context='worker source generations',
    )
    return WorkerAssignment(
        worker_id=_string(fields['worker_id'], context='worker assignment id'),
        spawn_instance=_string(
            fields['spawn_instance'],
            context='worker assignment spawn',
        ),
        source_generations=source_pairs,
        group_assignments=group_assignments,
        source_group_assignments=source_group_assignments,
        source_endpoints=_decode_source_endpoints(fields['source_endpoints']),
        event_grants=_decode_event_grants(fields['event_grants']),
    )


def _decode_control_payload(
    kind: str,
    fields: Mapping[str, object],
) -> ControlMessage:
    """Dispatch one validated closed control payload."""
    if kind == 'hello':
        message: ControlMessage = _decode_hello(fields)
    elif kind == 'assignment':
        message = _decode_worker_assignment(fields)
    elif kind == 'group-recovery-ready':
        message = _decode_group_recovery_ready(fields)
    elif kind == 'source-endpoint-ready':
        message = _decode_source_endpoint(fields)
    elif kind == 'event-peer-grant':
        message = _decode_event_peer_grant(fields)
    elif kind == 'event-peer-ready':
        message = _decode_event_peer_ready(fields)
    elif kind == 'stop':
        message = _decode_stop(fields)
    elif kind == 'health':
        message = _decode_health_update(fields)
    elif kind == 'operator-command':
        message = _decode_operator_command(fields)
    else:
        message = _decode_command_result(fields)
    return message


def _decode_control_body(body: bytes) -> ControlMessage:
    """Decode one closed versioned control body after its frame size was bounded."""
    envelope = _decode_json(body, context='control frame')
    _exact_fields(
        envelope,
        {'kind', 'payload', 'schema', 'version'},
        context='control frame',
    )
    if envelope['schema'] != _CONTROL_SCHEMA:
        raise _FrameError('control frame schema conflicts')
    if envelope['version'] != _CONTROL_PROTOCOL_VERSION:
        raise _FrameError('control frame version conflicts')
    kind = envelope['kind']
    if not isinstance(kind, str) or kind not in _CONTROL_KINDS:
        raise _FrameError('control frame kind conflicts')
    payload = envelope['payload']
    if not isinstance(payload, dict):
        raise _FrameError('control frame payload must be an object')
    return _decode_control_payload(kind, cast('dict[str, object]', payload))


def _unpack_length_frame(value: bytes, *, max_frame_bytes: int) -> bytes:
    """Return one exact length-prefixed frame body."""
    length = _frame_length(value, max_frame_bytes=max_frame_bytes)
    if length is None:
        raise _FrameError('length-prefixed frame is truncated')
    if len(value) != _LENGTH_BYTES + length:
        raise _FrameError('length-prefixed frame size conflicts')
    return value[_LENGTH_BYTES:]


def _decode_event_envelope(
    frame: bytes,
) -> tuple[str, dict[str, object], bytes]:
    """Decode and validate one event envelope around opaque payload bytes."""
    return _decode_event_body(_unpack_length_frame(frame, max_frame_bytes=2**32 - 1))


def _wire_identity(payload: Mapping[str, object]) -> LiveJoinIdentity:
    """Decode one complete strict live-join identity."""
    _exact_fields(
        payload,
        {
            'attempt_token',
            'boundary',
            'group',
            'group_generation',
            'join_kind',
            'site',
            'source',
            'source_generation',
        },
        context='strict identity',
    )
    join_kind = payload['join_kind']
    if not isinstance(join_kind, str) or join_kind not in {'eager', 'strict'}:
        raise _FrameError('strict identity join kind conflicts')
    boundary = payload['boundary']
    group_generation = payload['group_generation']
    if type(boundary) is not int or boundary < 0:
        raise _FrameError('strict identity boundary must be nonnegative')
    if type(group_generation) is not int or group_generation < 0:
        raise _FrameError('strict identity generation must be nonnegative')
    return LiveJoinIdentity(
        site=_string(payload['site'], context='strict identity site'),
        source=_string(payload['source'], context='strict identity source'),
        group=_string(payload['group'], context='strict identity group'),
        join_kind=cast("Literal['eager', 'strict']", join_kind),
        source_generation=_string(
            payload['source_generation'],
            context='strict identity source generation',
        ),
        group_generation=group_generation,
        boundary=boundary,
        attempt_token=_string(
            payload['attempt_token'],
            context='strict identity token',
        ),
    )


def _decode_identity_join_call(
    kind: str,
    fields: Mapping[str, object],
    request_id: int,
) -> _JoinCall:
    """Decode one identity-bearing join lifecycle call."""
    identity = fields.get('identity')
    if not isinstance(identity, dict):
        raise _FrameError('join request identity must be an object')
    decoded_identity = _wire_identity(cast('dict[str, object]', identity))
    if kind == 'join-confirm':
        _exact_fields(fields, {'identity', 'request_id'}, context='join confirm')
        value: (
            JoinAbort | JoinConfirm | JoinFinalize | StrictCapacityControl | StrictOrderingControl
        ) = JoinConfirm(decoded_identity)
    elif kind == 'join-finalize':
        _exact_fields(fields, {'identity', 'request_id'}, context='join finalize')
        value = JoinFinalize(decoded_identity)
    elif kind == 'join-abort':
        _exact_fields(fields, {'identity', 'request_id'}, context='join abort')
        value = JoinAbort(decoded_identity)
    else:
        value = _decode_join_control(fields, decoded_identity)
    return _JoinCall(request_id, value)


def _decode_join_call(body: bytes) -> _JoinCall:
    """Decode one unprefixed lane-to-source call body."""
    kind, fields, payload = _decode_event_body(body)
    if payload:
        raise _FrameError('join request contains a payload')
    if kind not in {
        'group-bootstrap-observe',
        'join-abort',
        'join-acknowledge',
        'join-confirm',
        'join-finalize',
        'join-request',
    }:
        raise _FrameError('event peer sent a non-request frame')
    request_id = _positive_int(
        fields.get('request_id'),
        context='join request id',
    )
    if kind == 'group-bootstrap-observe':
        bootstrap_fields = dict(fields)
        del bootstrap_fields['request_id']
        return _JoinCall(request_id, _decode_bootstrap_request(bootstrap_fields))
    if kind == 'join-request':
        return _decode_join_request(fields, request_id)
    return _decode_identity_join_call(kind, fields, request_id)


def _decode_join_target(mapping: Mapping[str, object]) -> JoinTarget:
    """Decode one source join-target observation."""
    _exact_fields(
        mapping,
        {
            'identity',
            'source_successful_clock',
            'stream_incarnation',
        },
        context='join target',
    )
    identity_payload = mapping['identity']
    incarnation = mapping['stream_incarnation']
    successful_clock = mapping['source_successful_clock']
    if not isinstance(identity_payload, dict):
        raise _FrameError('join target identity must be an object')
    if incarnation is not None and not isinstance(incarnation, str):
        raise _FrameError('join target incarnation must be text or null')
    normalized_clock: float | None = None
    if successful_clock is not None:
        normalized_clock = _finite_float(
            successful_clock,
            context='join target successful clock must be numeric or null',
        )
        if normalized_clock < 0:
            raise _FrameError('join target successful clock must be numeric or null')
    return JoinTarget(
        _wire_identity(cast('dict[str, object]', identity_payload)),
        incarnation,
        normalized_clock,
    )


def _decode_join_result(result_kind: object, payload: object) -> _JoinResult:
    """Decode one nonempty closed join result variant."""
    if not isinstance(payload, dict):
        raise _FrameError('join reply result must be an object')
    mapping = cast('dict[str, object]', payload)
    if result_kind == 'bootstrap-observation':
        result: _JoinResult = _decode_bootstrap_observation(mapping)
    elif result_kind == 'bootstrap-pending':
        result = _decode_bootstrap_pending(mapping)
    elif result_kind == 'retry':
        result = _decode_join_retry(mapping)
    elif result_kind == 'target':
        result = _decode_join_target(mapping)
    elif result_kind == 'attached':
        result = Attached(_wire_identity(mapping))
    elif result_kind == 'finalized':
        result = JoinFinalized(_wire_identity(mapping))
    else:
        raise _FrameError('join reply result kind conflicts')
    return result


def _decode_join_reply(fields: Mapping[str, object]) -> _JoinReply:
    """Decode one closed request-correlated source reply."""
    _exact_fields(
        fields,
        {'error', 'request_id', 'result', 'result_kind'},
        context='join reply',
    )
    request_id = _positive_int(fields['request_id'], context='join reply id')
    error = fields['error']
    if error is not None and not isinstance(error, str):
        raise _FrameError('join reply error must be text or null')
    result_kind = fields['result_kind']
    result_payload = fields['result']
    if result_kind == 'none':
        if result_payload is not None:
            raise _FrameError('empty join reply contains a result')
        result: _JoinResult = None
    else:
        result = _decode_join_result(result_kind, result_payload)
    return _JoinReply(
        _request_id=request_id,
        _result=result,
        _error=error,
    )


def _decode_strict_capacity(
    fields: Mapping[str, object],
) -> StrictCapacityControl:
    """Decode one body-free strict-capacity control."""
    _exact_fields(
        fields,
        {'abort_limit', 'identity'},
        context='strict capacity metadata',
    )
    identity = fields['identity']
    if not isinstance(identity, dict):
        raise _FrameError('strict capacity identity must be an object')
    return _decode_strict_capacity_fields(
        fields,
        _wire_identity(cast('dict[str, object]', identity)),
    )


def _decode_strict_ordering(
    fields: Mapping[str, object],
) -> StrictOrderingControl:
    """Decode one body-free strict-ordering control."""
    _exact_fields(
        fields,
        {'action', 'delivery_floor', 'event_id', 'identity', 'last_delivered_id'},
        context='strict ordering metadata',
    )
    identity = fields['identity']
    if not isinstance(identity, dict):
        raise _FrameError('strict ordering identity must be an object')
    return _decode_strict_ordering_fields(
        fields,
        _wire_identity(cast('dict[str, object]', identity)),
    )


def _decode_body_free_event(
    kind: str,
    fields: Mapping[str, object],
) -> StrictCapacityControl | StrictOrderingControl | RangeChanged | _PeerDetach:
    """Decode one body-free event-channel control."""
    if kind == 'detach':
        _exact_fields(fields, {'reason'}, context='detach metadata')
        return _PeerDetach(_reason=_string(fields['reason'], context='detach reason'))
    if kind == 'strict-capacity':
        return _decode_strict_capacity(fields)
    if kind == 'strict-ordering':
        return _decode_strict_ordering(fields)
    if kind == 'range-changed':
        _exact_fields(fields, set(), context='range-changed metadata')
        return RangeChanged()
    raise _FrameError('event frame kind requires a payload')


def _decode_event_wire(
    frame: bytes,
    *,
    coordination_only: bool = False,
) -> FanoutMessage | _JoinReply | _PeerDetach:
    """Decode one event-channel frame, including peer detach."""
    kind, fields, payload = _decode_event_envelope(frame)
    if coordination_only and kind not in {'detach', 'join-reply'}:
        raise _FrameError('coordination stream received a fanout frame')
    if kind == 'join-reply':
        if payload:
            raise _FrameError('join reply contains a payload')
        return _decode_join_reply(fields)
    if kind == 'event':
        return _decode_event_delivery(fields, payload)
    if payload:
        raise _FrameError('body-free event control contains a payload')
    return _decode_body_free_event(kind, fields)


def encode_control_message(message: ControlMessage) -> bytes:
    """Encode one typed worker-control message.

    Args:
        message: Closed internal control value.

    Returns:
        Complete length-prefixed canonical frame.
    """
    kind, payload = _message_payload(message)
    body = _encode_json(
        {
            'kind': kind,
            'payload': payload,
            'schema': _CONTROL_SCHEMA,
            'version': _CONTROL_PROTOCOL_VERSION,
        }
    )
    return struct.pack('!I', len(body)) + body
