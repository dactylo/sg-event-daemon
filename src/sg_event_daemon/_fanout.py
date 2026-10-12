"""Immutable source frames, dedicated encoding, and bounded group fanout."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import queue
import threading
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Protocol, TypeAlias

from ._event_frame import event_frame_bytes
from ._events import Event, encode_event

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable
    from types import TracebackType
    from typing import Self

    from ._group_types import LiveJoinIdentity

_EncoderHaltCode: TypeAlias = Literal[
    'channel-detached',
    'encoder-died',
    'encoder-failed',
    'encoder-shutdown',
    'event-too-large',
    'frame-too-large',
]


FanoutMessage: TypeAlias = """(
    EventDelivery | RangeChanged |
    StrictCapacityControl | StrictOrderingControl
)"""


@dataclass(frozen=True)
class EncodedEvent:
    """Hold one immutable encoded event frame and its exact wire byte count."""

    event_id: int
    event_frame: bytes
    byte_count: int = field(init=False)

    def __post_init__(self) -> None:
        """Validate event identity and record complete framed accounting.

        Raises:
            ValueError: `event_id` is not an exact positive integer.
            TypeError: `event_frame` is not exactly `bytes`.
        """
        if type(self.event_id) is not int or self.event_id <= 0:
            raise ValueError('encoded event id must be a positive integer')
        if type(self.event_frame) is not bytes:
            raise TypeError('encoded event frame must be bytes')
        kind, metadata, payload = self.wire_parts()
        object.__setattr__(
            self, 'byte_count', event_frame_bytes(kind, metadata, payload_byte_count=len(payload))
        )

    @classmethod
    def from_event_frame(cls, event_id: int, event_frame: bytes) -> Self:
        """Create one immutable complete-frame accounting value.

        Args:
            event_id: Exact positive EventLogEntry id.
            event_frame: Immutable encoded event bytes.

        Returns:
            An encoded event with its exact complete wire byte count.
        """
        return cls(event_id=event_id, event_frame=event_frame)

    def wire_parts(self) -> tuple[str, dict[str, object], bytes]:
        """Return the event kind, metadata, and encoded payload."""
        return 'event', {'event_id': self.event_id}, self.event_frame


class EncoderHaltedError(RuntimeError):
    """Report a terminal event-encoding halt.

    `code` identifies the terminal reason independently of the diagnostic.
    """

    def __init__(self, message: str, *, code: _EncoderHaltCode | None = None) -> None:
        """Keep the diagnostic separate from the machine-readable reason.

        Args:
            message: Human-readable diagnostic preserved by `str(error)`.
            code: Stable reason for control flow; `None` is unclassified.
        """
        super().__init__(message)
        self.code = code


class _FrameTooLargeError(EncoderHaltedError):
    """Report an event frame body that exceeds the IPC limit."""


@dataclass(frozen=True)
class EventDelivery:
    """Carry the only fanout message that contains an event body."""

    encoded: EncodedEvent

    @property
    def byte_count(self) -> int:
        """Return complete framed bytes charged to bounded queues.

        Returns:
            Exact complete event frame bytes.
        """
        return self.encoded.byte_count

    def wire_parts(self) -> tuple[str, dict[str, object], bytes]:
        """Return the event kind, metadata, and already encoded payload."""
        return self.encoded.wire_parts()


@dataclass(frozen=True)
class FanoutStatus:
    """Describe one bounded group channel without retaining event bodies."""

    group: str
    ordering: Literal['eager', 'strict']
    attached: bool
    provisional: bool
    queued_count: int
    queued_bytes: int
    reason: str | None


@dataclass(frozen=True, slots=True)
class RangeChanged:
    """Hint that a source range opened, merged, or closed without evidence."""

    byte_count: int = field(init=False)

    def __post_init__(self) -> None:
        """Cache the exact complete wire charge."""
        kind, metadata, payload = self.wire_parts()
        object.__setattr__(
            self, 'byte_count', event_frame_bytes(kind, metadata, payload_byte_count=len(payload))
        )

    def wire_parts(self) -> tuple[str, dict[str, object], bytes]:
        """Return the body-free range hint kind, metadata, and payload."""
        return 'range-changed', {}, b''


class GroupChannelInterface(Protocol):
    """Expose the group channel operations shared by local and remote fanout."""

    def activate(self, *, provisional: bool) -> None:
        """Activate a live or provisional attachment."""
        ...

    def close(self) -> None:
        """Release the channel."""
        ...

    def detach(self, reason: str) -> None:
        """Record the first detach reason."""
        ...

    def drain(self) -> tuple[FanoutMessage, ...]:
        """Remove and return locally buffered messages, if any."""
        ...

    @property
    def group(self) -> str:
        """Return the group identity."""
        ...

    def offer(self, message: FanoutMessage) -> bool:
        """Offer a frame immediately, detaching if queue capacity is exhausted.

        Returns:
            `True` when accepted; `False` when the channel is detached.
        """
        ...

    def offer_hint(self, message: RangeChanged) -> bool:
        """Offer a coalesced hint without detaching or charging event capacity.

        Returns:
            `True` when accepted or coalesced with a pending hint. `False` when
            the attachment is unavailable or transport capacity prevents
            acceptance; capacity rejection does not detach the channel.
        """
        ...

    def offer_paced(self, message: FanoutMessage) -> bool:
        """Offer a strict frame without detaching for temporary queue pressure.

        Returns:
            `True` when accepted; `False` when detached or currently full.
        """
        ...

    @property
    def ordering(self) -> Literal['eager', 'strict']:
        """Return the configured delivery order."""
        ...

    def promote(self) -> None:
        """Complete a provisional attachment."""
        ...

    @property
    def status(self) -> FanoutStatus:
        """Return the body-free queue status."""
        ...


class GroupChannel:
    """Own one nonblocking count-and-byte-bounded group message queue."""

    def __init__(
        self,
        *,
        group: str,
        ordering: Literal['eager', 'strict'],
        max_events: int,
        max_bytes: int,
    ) -> None:
        """Create one empty attached bounded channel.

        Args:
            group: Stable group identity.
            ordering: Eager direct-fanout or strict release mode.
            max_events: Maximum queued frames, including barriers.
            max_bytes: Maximum complete queued frame bytes.

        Raises:
            ValueError: The group is empty or either queue bound is not
                positive.
        """
        if not group:
            raise ValueError('group channel identity must not be empty')
        if max_events <= 0 or max_bytes <= 0:
            raise ValueError('group channel limits must be positive')
        self._attached = True
        self._group = group
        self._lock = threading.Lock()
        self._max_bytes = max_bytes
        self._max_events = max_events
        self._ordering: Literal['eager', 'strict'] = ordering
        self._provisional = False
        self._queue: list[FanoutMessage] = []
        self._queued_bytes = 0
        self._hint_pending: RangeChanged | None = None
        self._reason: str | None = None

    def _detach_locked(self, reason: str) -> None:
        """Detach and release all queued bodies while holding the lock."""
        if not self._attached:
            return
        self._attached = False
        self._provisional = False
        self._queue.clear()
        self._queued_bytes = 0
        self._hint_pending = None
        self._reason = reason

    def activate(self, *, provisional: bool) -> None:
        """Activate this channel for one live or provisional attachment.

        Discard all queued messages and clear the previous detach reason, even
        when the channel is already attached.

        Args:
            provisional: Whether the live join still owes its durable
                compare-and-swap.
        """
        with self._lock:
            self._attached = True
            self._provisional = provisional
            self._queue.clear()
            self._queued_bytes = 0
            self._hint_pending = None
            self._reason = None

    def close(self) -> None:
        """Detach idempotently and release every queued event body."""
        with self._lock:
            self._detach_locked('channel-closed')

    def detach(self, reason: str) -> None:
        """Detach idempotently for the first recorded reason.

        Args:
            reason: Stable body-free detach condition.
        """
        with self._lock:
            self._detach_locked(reason)

    def drain(self) -> tuple[FanoutMessage, ...]:
        """Remove and return every currently queued message.

        Returns:
            Queued frames in enqueue order, followed by at most one coalesced
            range-change hint.
        """
        with self._lock:
            messages = tuple(self._queue) + (
                (self._hint_pending,) if self._hint_pending is not None else ()
            )
            self._queue.clear()
            self._queued_bytes = 0
            self._hint_pending = None
            return messages

    @property
    def group(self) -> str:
        """Return the stable group name.

        Returns:
            Group identity.
        """
        return self._group

    def offer(self, message: FanoutMessage) -> bool:
        """Offer one frame without waiting for queue space.

        Args:
            message: Immutable event delivery or body-free barrier.

        Returns:
            `True` when queued. `False` after closure or capacity detachment.
        """
        with self._lock:
            if not self._attached:
                return False
            if len(self._queue) + 1 > self._max_events:
                self._detach_locked('queue-count-exhausted')
                return False
            if self._queued_bytes + message.byte_count > self._max_bytes:
                self._detach_locked('queue-byte-exhausted')
                return False
            self._queue.append(message)
            self._queued_bytes += message.byte_count
            return True

    def offer_hint(self, message: RangeChanged) -> bool:
        """Coalesce one advisory hint outside bounded event capacity.

        Returns:
            `True` when retained or coalesced with a pending hint, even when
            the event queue is full. `False` when the channel is detached.
        """
        with self._lock:
            if not self._attached:
                return False
            if self._hint_pending is None:
                self._hint_pending = message
            return True

    def offer_paced(self, message: FanoutMessage) -> bool:
        """Offer one strict frame without detaching at current capacity.

        Args:
            message: Immutable strict event or body-free control frame.

        Returns:
            `True` when queued. `False` after closure or while space is
            unavailable.

        Raises:
            ValueError: This is not a strict group channel.
        """
        with self._lock:
            if self._ordering != 'strict':
                raise ValueError('paced offers require a strict group channel')
            if not self._attached:
                return False
            if (
                len(self._queue) + 1 > self._max_events
                or self._queued_bytes + message.byte_count > self._max_bytes
            ):
                return False
            self._queue.append(message)
            self._queued_bytes += message.byte_count
            return True

    @property
    def ordering(self) -> Literal['eager', 'strict']:
        """Return this channel's delivery ordering.

        Returns:
            Eager or strict.
        """
        return self._ordering

    def promote(self) -> None:
        """Promote an exact provisional channel to joined.

        Raises:
            EncoderHaltedError: The channel is detached.
        """
        with self._lock:
            if not self._attached:
                raise EncoderHaltedError(
                    'cannot promote a detached channel', code='channel-detached'
                )
            self._provisional = False

    @property
    def status(self) -> FanoutStatus:
        """Return a body-free immutable queue status.

        Returns:
            Current attachment, capacity, and detach state.
        """
        with self._lock:
            return FanoutStatus(
                group=self._group,
                ordering=self._ordering,
                attached=self._attached,
                provisional=self._provisional,
                queued_count=len(self._queue),
                queued_bytes=self._queued_bytes,
                reason=self._reason,
            )


class EventFanout:
    """Coordinate ordered nonblocking delivery to independent group queues."""

    def __init__(self) -> None:
        """Create an empty synchronized fanout registry."""
        self._channels: dict[str, GroupChannelInterface] = {}
        self._lock = threading.RLock()

    def attach(self, channel: GroupChannelInterface, *, provisional: bool = False) -> None:
        """Attach and activate one group's exact channel.

        An existing detached channel may be replaced. Attaching the same object
        activates it again; a `GroupChannel` discards its queued messages on
        activation.

        Args:
            channel: Bounded group channel.
            provisional: Whether a later join finalize must promote it.

        Raises:
            ValueError: A different attached channel already owns this group.
        """
        with self._lock:
            existing = self._channels.get(channel.group)
            if existing is not None and existing is not channel and existing.status.attached:
                raise ValueError(f'group channel already attached: {channel.group}')
            channel.activate(provisional=provisional)
            self._channels[channel.group] = channel

    def channel(self, group: str) -> GroupChannelInterface | None:
        """Return the currently registered channel for one group.

        Args:
            group: Stable group name.

        Returns:
            Exact channel, or `None` when never registered.
        """
        with self._lock:
            return self._channels.get(group)

    def detach(self, group: str, *, reason: str) -> None:
        """Detach one group without affecting any peer.

        Args:
            group: Stable group identity.
            reason: First body-free detach reason.
        """
        with self._lock:
            channel = self._channels.get(group)
            if channel is not None:
                channel.detach(reason)

    @contextmanager
    def locked(self) -> Generator[None]:
        """Hold the fanout lock across a frontier capture.

        Yields:
            Control while attachment and publication are serialized.
        """
        with self._lock:
            yield

    def promote(self, group: str, channel: GroupChannelInterface) -> None:
        """Promote only the currently registered provisional channel.

        Args:
            group: Stable group identity.
            channel: Exact token-owned provisional channel.

        Raises:
            ValueError: Another attempt owns the registered channel.
            EncoderHaltedError: The registered in-memory channel is detached.
            ConnectionError: The registered remote channel is detached.
        """
        with self._lock:
            if self._channels.get(group) is not channel:
                raise ValueError('join attempt no longer owns group channel')
            channel.promote()

    def publish_events(self, events: Iterable[EncodedEvent]) -> tuple[FanoutStatus, ...]:
        """Publish one committed event chunk to eager channels.

        Args:
            events: Immutable encoded events in source order; strict channels
                use their buffer.

        Returns:
            Statuses for every registered channel, including strict and
            detached channels, sorted by group.
        """
        with self._lock:
            deliveries = tuple(EventDelivery(encoded) for encoded in events)
            channels = tuple(self._channels.values())
            for channel in channels:
                _publish_channel(channel, deliveries)
            return tuple(
                channel.status for channel in sorted(channels, key=lambda item: item.group)
            )

    def publish_range_changed(self) -> None:
        """Offer one coalesced advisory hint to each attached channel."""
        with self._lock:
            hint = RangeChanged()
            for channel in self._channels.values():
                if channel.status.attached:
                    channel.offer_hint(hint)


@dataclass(frozen=True)
class StrictCapacityControl:
    """Carry one complete-identity body-free live-capacity halt."""

    identity: LiveJoinIdentity
    abort_limit: int
    byte_count: int = field(init=False)

    def __post_init__(self) -> None:
        """Validate the strict identity and cache bounded frame bytes.

        Raises:
            ValueError: The identity is not strict or `abort_limit` is not an
                exact positive integer.
        """
        if self.identity.join_kind != 'strict':
            raise ValueError('strict capacity control requires a strict identity')
        if type(self.abort_limit) is not int or self.abort_limit <= 0:
            raise ValueError('strict capacity abort limit must be positive')
        kind, metadata, payload = self.wire_parts()
        object.__setattr__(
            self, 'byte_count', event_frame_bytes(kind, metadata, payload_byte_count=len(payload))
        )

    def wire_parts(self) -> tuple[str, dict[str, object], bytes]:
        """Return the capacity control kind, metadata, and empty payload."""
        return 'strict-capacity', strict_control_metadata(self), b''


@dataclass(frozen=True)
class StrictOrderingControl:
    """Carry one complete-identity body-free ordering halt or floor skip."""

    identity: LiveJoinIdentity
    event_id: int
    last_delivered_id: int
    delivery_floor: int
    action: Literal['halt', 'skip']
    byte_count: int = field(init=False)

    def __post_init__(self) -> None:
        """Validate the decision and cache its bounded frame bytes.

        Raises:
            ValueError: The identity is not strict, `event_id` is not an exact
                positive integer, `last_delivered_id` or `delivery_floor` is
                not an exact nonnegative integer, or `action` is neither `halt`
                nor `skip`.
        """
        if self.identity.join_kind != 'strict':
            raise ValueError('strict ordering control requires a strict identity')
        if type(self.event_id) is not int or self.event_id <= 0:
            raise ValueError('strict ordering event id must be positive')
        if type(self.last_delivered_id) is not int or self.last_delivered_id < 0:
            raise ValueError('strict last-delivered id must be nonnegative')
        if type(self.delivery_floor) is not int or self.delivery_floor < 0:
            raise ValueError('strict delivery floor must be nonnegative')
        if self.action not in {'halt', 'skip'}:
            raise ValueError('strict ordering action is invalid')
        kind, metadata, payload = self.wire_parts()
        object.__setattr__(
            self, 'byte_count', event_frame_bytes(kind, metadata, payload_byte_count=len(payload))
        )

    def wire_parts(self) -> tuple[str, dict[str, object], bytes]:
        """Return the ordering control kind, metadata, and empty payload."""
        return 'strict-ordering', strict_control_metadata(self), b''


@dataclass(frozen=True)
class _EncodeFailure:
    """Carry one normalized terminal encoder failure."""

    _code: _EncoderHaltCode
    _error: BaseException


class EncoderWorker:
    """Own one dedicated encoder thread with one synchronous request slot.

    A terminal encoding failure makes the worker unusable for further requests.
    Call `close()` or use the context manager to request shutdown and release
    waiting callers. Shutdown waits only a bounded time, so an encoder blocked
    inside the supplied encoding function may remain alive afterward.
    """

    def __init__(
        self,
        *,
        max_event_bytes: int,
        max_ipc_frame_bytes: int,
        event_encoder: Callable[[Event], bytes] = encode_event,
    ) -> None:
        """Start the bounded dedicated encoder.

        Args:
            max_event_bytes: Maximum encoded event bytes.
            max_ipc_frame_bytes: Maximum event frame body bytes.
            event_encoder: Pure event encoder run only by the worker thread.

        Raises:
            ValueError: Either byte limit is not positive.
        """
        if max_event_bytes <= 0 or max_ipc_frame_bytes <= 0:
            raise ValueError('encoder byte limits must be positive')
        self._closed = threading.Event()
        self._event_encoder = event_encoder
        self._call_lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._max_event_bytes = max_event_bytes
        self._max_ipc_frame_bytes = max_ipc_frame_bytes
        self._requests: queue.Queue[_EncodeRequest | None] = queue.Queue(maxsize=1)
        self._results: queue.Queue[EncodedEvent | _EncodeFailure] = queue.Queue(maxsize=1)
        self._terminal: _EncodeFailure | None = None
        self._thread = threading.Thread(
            target=self._run,
            name='sg-event-encoder',
            daemon=True,
        )
        self._thread.start()

    def __enter__(self) -> Self:
        """Return this live worker.

        Returns:
            This encoder until context exit.
        """
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Request bounded encoder shutdown at context exit."""
        self.close()

    def _encode_event(self, event: Event) -> EncodedEvent | _EncodeFailure:
        """Encode and validate one event on the dedicated worker."""
        raw_id = event['id']
        if type(raw_id) is not int or raw_id <= 0:
            raise ValueError('event id must be a positive integer')
        frame = self._event_encoder(event)
        if type(frame) is not bytes:
            raise TypeError('event encoder returned non-bytes')
        if len(frame) > self._max_event_bytes:
            return _EncodeFailure(
                'event-too-large',
                ValueError(
                    f'encoded event uses {len(frame)} bytes; limit is {self._max_event_bytes}'
                ),
            )
        encoded = EncodedEvent.from_event_frame(raw_id, frame)
        framed_bytes = encoded.byte_count - 4
        if framed_bytes > self._max_ipc_frame_bytes:
            return _EncodeFailure(
                'frame-too-large',
                ValueError(
                    f'frame body uses {framed_bytes} bytes; limit is {self._max_ipc_frame_bytes}'
                ),
            )
        return encoded

    def _failure_error(self, failure: _EncodeFailure) -> EncoderHaltedError:
        """Build the public terminal error for one worker failure."""
        message = f'{failure._code}: {failure._error}'
        if failure._code == 'frame-too-large':
            return _FrameTooLargeError(message, code=failure._code)
        return EncoderHaltedError(message, code=failure._code)

    def _publish_result(self, result: EncodedEvent | _EncodeFailure) -> None:
        """Publish one result without blocking shutdown indefinitely."""
        while not self._closed.is_set():
            try:
                self._results.put(result, timeout=0.01)
            except queue.Full:
                continue
            else:
                return

    def _record_failure(self, failure: _EncodeFailure) -> None:
        """Retain the first terminal worker failure."""
        with self._lifecycle_lock:
            if self._terminal is None:
                self._terminal = failure

    def _require_usable(self) -> None:
        """Raise the stable reason this encoder cannot accept work."""
        with self._lifecycle_lock:
            if self._closed.is_set():
                raise EncoderHaltedError('encoder-shutdown', code='encoder-shutdown')
            failure = self._terminal
            if failure is None and not self._thread.is_alive():
                failure = _EncodeFailure(
                    'encoder-died',
                    RuntimeError('encoder worker stopped without a terminal result'),
                )
                self._terminal = failure
        if failure is not None:
            self._thread.join(timeout=0.05)
            raise self._failure_error(failure)

    def _receive(self) -> EncodedEvent:
        """Wait interruptibly for the submitted request's result."""
        while True:
            try:
                result = self._results.get(timeout=0.01)
            except queue.Empty:
                self._require_usable()
            else:
                if isinstance(result, _EncodeFailure):
                    self._thread.join(timeout=0.05)
                    raise self._failure_error(result)
                return result

    def _serve(self) -> None:
        """Serve requests until shutdown or a terminal result."""
        while not self._closed.is_set():
            try:
                request = self._requests.get(timeout=0.01)
            except queue.Empty:
                continue
            if request is None:
                return
            try:
                result = self._encode_event(request._event)
            except BaseException as exc:
                code = 'encoder-failed' if isinstance(exc, Exception) else 'encoder-died'
                failure = _EncodeFailure(code, exc)
                self._record_failure(failure)
                self._publish_result(failure)
                raise
            if isinstance(result, _EncodeFailure):
                self._record_failure(result)
                self._publish_result(result)
                return
            self._publish_result(result)

    def _run(self) -> None:
        """Process requests serially on the dedicated daemon thread."""
        try:
            self._serve()
        except BaseException as exc:
            with self._lifecycle_lock:
                failure = self._terminal
            if failure is None or failure._error is not exc:
                raise

    def _submit(self, event: Event) -> None:
        """Submit one event while remaining interruptible by shutdown."""
        request = _EncodeRequest(event)
        while True:
            try:
                self._requests.put(request, timeout=0.01)
            except queue.Full:
                self._require_usable()
            else:
                return

    def close(self) -> None:
        """Interrupt callers and request bounded worker shutdown."""
        with self._lifecycle_lock:
            if self._closed.is_set():
                return
            self._closed.set()
        with suppress(queue.Full):
            self._requests.put_nowait(None)
        if threading.current_thread() is not self._thread:
            self._thread.join(timeout=0.05)

    def encode(self, event: Event) -> EncodedEvent:
        """Encode one event or raise a stable terminal halt.

        Args:
            event: Immutable provider event.

        Returns:
            Immutable encoded frame and exact bounded-memory accounting.

        Raises:
            EncoderHaltedError: Encoding failed, died, was interrupted, or
                produced an event frame body that exceeds IPC policy.
        """
        with self._call_lock:
            self._require_usable()
            self._submit(event)
            return self._receive()


@dataclass(frozen=True)
class _EncodeRequest:
    """Carry one event through the bounded encoder request slot."""

    _event: Event


def _publish_channel(
    channel: GroupChannelInterface, deliveries: tuple[EventDelivery, ...]
) -> None:
    """Publish one committed chunk to an eager group until its queue rejects."""
    if channel.ordering == 'strict':
        return
    for delivery in deliveries:
        if not channel.offer(delivery):
            return


def strict_control_metadata(
    control: StrictCapacityControl | StrictOrderingControl,
) -> dict[str, object]:
    """Encode the shared strict-control fields for event and join frames.

    Args:
        control: Validated capacity or ordering control.

    Returns:
        Metadata in the existing event-channel field order.
    """
    if isinstance(control, StrictCapacityControl):
        return {
            'abort_limit': control.abort_limit,
            'identity': control.identity.wire_payload(),
        }
    return {
        'action': control.action,
        'delivery_floor': control.delivery_floor,
        'event_id': control.event_id,
        'identity': control.identity.wire_payload(),
        'last_delivered_id': control.last_delivered_id,
    }
