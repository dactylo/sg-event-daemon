"""Immutable in-memory supervisor progress and probe state."""

from __future__ import annotations

import ipaddress
import math
import socket
import threading
from contextlib import suppress
from dataclasses import dataclass
from math import isfinite
from time import monotonic
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

_BAD_REQUEST = (
    b'HTTP/1.1 400 Bad Request\r\n'
    b'Content-Type: text/plain; charset=us-ascii\r\n'
    b'Content-Length: 12\r\n'
    b'Connection: close\r\n'
    b'\r\n'
    b'bad request\n'
)
_BAD_REQUEST_HEAD = _BAD_REQUEST.removesuffix(b'bad request\n')
_IPV4_VERSION = 4
_MAX_HEADER_BYTES = 8_192
_MAX_PROBE_PEERS = 16
_MAX_REQUEST_LINE_BYTES = 1_024
_METHOD_NOT_ALLOWED = (
    b'HTTP/1.1 405 Method Not Allowed\r\n'
    b'Content-Type: text/plain; charset=us-ascii\r\n'
    b'Content-Length: 19\r\n'
    b'Connection: close\r\n'
    b'\r\n'
    b'method not allowed\n'
)
_NOT_FOUND = (
    b'HTTP/1.1 404 Not Found\r\n'
    b'Content-Type: text/plain; charset=us-ascii\r\n'
    b'Content-Length: 10\r\n'
    b'Connection: close\r\n'
    b'\r\n'
    b'not found\n'
)
_NOT_FOUND_HEAD = _NOT_FOUND.removesuffix(b'not found\n')
_OK = (
    b'HTTP/1.1 200 OK\r\n'
    b'Content-Type: text/plain; charset=us-ascii\r\n'
    b'Content-Length: 3\r\n'
    b'Connection: close\r\n'
    b'\r\n'
    b'ok\n'
)
_OK_HEAD = _OK.removesuffix(b'ok\n')
_PEER_TIMEOUT_SECS = 1.0
_PROBE_BACKLOG = 16
_UNAVAILABLE = (
    b'HTTP/1.1 503 Service Unavailable\r\n'
    b'Content-Type: text/plain; charset=us-ascii\r\n'
    b'Content-Length: 12\r\n'
    b'Connection: close\r\n'
    b'\r\n'
    b'unavailable\n'
)
_UNAVAILABLE_HEAD = _UNAVAILABLE.removesuffix(b'unavailable\n')


@dataclass(frozen=True)
class ProbeResult:
    """Describe one lock-bounded liveness and readiness observation."""

    liveness: bool
    readiness: bool
    complete: bool
    conditions: tuple[str, ...]
    generated_at: float | None
    snapshot_age_seconds: float | None

    def __post_init__(self) -> None:
        """Validate the closed immutable result shape.

        Raises:
            TypeError: `conditions` is not exactly a tuple.
            ValueError: A condition is not a nonempty string, or a supplied
                `generated_at` or `snapshot_age_seconds` is not a finite
                nonnegative integer or float.
        """
        if type(self.conditions) is not tuple:
            raise TypeError('probe conditions must be a tuple')
        if self.generated_at is not None:
            _validate_time(self.generated_at, 'probe generation')
        if self.snapshot_age_seconds is not None:
            _validate_time(self.snapshot_age_seconds, 'probe snapshot age')
        if any(type(condition) is not str or not condition for condition in self.conditions):
            raise ValueError('probe conditions must be nonempty text')


class ProbeServer:
    """Own an HTTP probe listener with at most 16 concurrent peers.

    Construction opens the listener. `start()` starts its accept thread, which
    admits peers on bounded daemon threads and closes excess connections.
    Result callbacks are serialized. `close()` idempotently closes the listener
    and shuts down active peers; their serving threads close the peer sockets.
    Use `join()` to wait for all owned threads to stop.
    """

    def __init__(
        self,
        bind: str,
        port: int,
        result: Callable[[], ProbeResult],
        *,
        peer_timeout_secs: float = _PEER_TIMEOUT_SECS,
    ) -> None:
        """Bind and listen on one endpoint with bounded concurrent peers.

        Args:
            bind: IPv4 or IPv6 address literal to bind.
            port: TCP port to bind.
            result: I/O-free request-time probe result callback.
            peer_timeout_secs: Finite positive peer read and write timeout in
                seconds.

        Raises:
            ValueError: The bind address is not an IP literal or the peer
                timeout is not finite and positive.
            OSError: The listener cannot be opened or bound.
        """
        if (
            type(peer_timeout_secs) not in {float, int}
            or not math.isfinite(peer_timeout_secs)
            or peer_timeout_secs <= 0
        ):
            raise ValueError('probe peer timeout must be positive and finite')
        address = ipaddress.ip_address(bind)
        family = socket.AF_INET if address.version == _IPV4_VERSION else socket.AF_INET6
        listener = socket.socket(family, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((bind, port))
            listener.listen(_PROBE_BACKLOG)
            listener.settimeout(0.1)
        except BaseException:
            listener.close()
            raise
        self._closing = False
        self._failure_reason: str | None = None
        self._listener = listener
        self._lock = threading.Lock()
        self._peer_threads: set[threading.Thread] = set()
        self._peer_timeout_secs = float(peer_timeout_secs)
        self._peers: set[socket.socket] = set()
        self._result = result
        self._result_lock = threading.Lock()
        self._thread: threading.Thread | None = None

    @staticmethod
    def _response(
        method: str,
        path: str,
        result: ProbeResult | None,
    ) -> bytes:
        """Return one fixed HTTP response for a parsed request."""
        is_head = method == 'HEAD'
        if method not in {'GET', 'HEAD'}:
            return _METHOD_NOT_ALLOWED
        if path not in {'/livez', '/readyz'}:
            return _NOT_FOUND_HEAD if is_head else _NOT_FOUND
        if result is None:
            raise RuntimeError('probe result is unavailable')
        available = result.liveness if path == '/livez' else result.readiness
        if available:
            return _OK_HEAD if is_head else _OK
        return _UNAVAILABLE_HEAD if is_head else _UNAVAILABLE

    def _serve_peer(self, peer: socket.socket) -> None:
        """Read and answer one peer within its total read and write budget."""
        with peer:
            deadline = monotonic() + self._peer_timeout_secs
            try:
                method, path = _read_request(peer, deadline=deadline)
            except _BadRequestError as error:
                response = (
                    _BAD_REQUEST_HEAD
                    if error._is_head  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
                    else _BAD_REQUEST
                )
            except OSError:
                return
            else:
                result = None
                if method in {'GET', 'HEAD'} and path in {'/livez', '/readyz'}:
                    with self._result_lock:
                        result = self._result()
                response = self._response(method, path, result)
            try:
                _set_peer_timeout(peer, deadline=deadline)
                peer.sendall(response)
            except (OSError, TimeoutError):
                return

    def _serve_admitted(self, peer: socket.socket) -> None:
        """Release an admitted peer and fail closed after unexpected errors."""
        served = False
        try:
            self._serve_peer(peer)
            served = True
        finally:
            with self._lock:
                if not served:
                    if not self._closing:
                        self._failure_reason = 'probe listener exited unexpectedly'
                    self._listener.close()
                self._peers.remove(peer)

    def _serve(self) -> None:
        """Accept peers onto bounded threads until closed or failed."""
        try:
            while True:
                try:
                    peer, _address = self._listener.accept()
                except TimeoutError:
                    with self._lock:
                        if self._closing:
                            return
                    continue
                except OSError:
                    return
                with self._lock:
                    self._peer_threads = {
                        thread for thread in self._peer_threads if thread.is_alive()
                    }
                    if (
                        self._closing
                        or self._failure_reason is not None
                        or len(self._peers) >= _MAX_PROBE_PEERS
                    ):
                        peer.close()
                        continue
                    thread = threading.Thread(
                        target=self._serve_admitted,
                        args=(peer,),
                        name='sg-event-probe-peer',
                        daemon=True,
                    )
                    started = False
                    try:
                        thread.start()
                        started = True
                    finally:
                        if started:
                            self._peer_threads.add(thread)
                            self._peers.add(peer)
                        else:
                            peer.close()
        finally:
            with self._lock:
                if not self._closing:
                    self._failure_reason = 'probe listener exited unexpectedly'
                self._listener.close()

    def close(self) -> None:
        """Close the listener and interrupt active peers for their owners to close."""
        with self._lock:
            if self._closing:
                return
            self._closing = True
            self._listener.close()
            for peer in self._peers:
                with suppress(OSError):
                    peer.shutdown(socket.SHUT_RDWR)

    @property
    def failure_reason(self) -> str | None:
        """Return a bounded reason after unexpected server exit.

        Returns:
            A fixed failure reason after unexpected exit, otherwise `None`.
        """
        with self._lock:
            return self._failure_reason

    def join(self, *, timeout: float) -> bool:
        """Return whether all owned threads stopped within the timeout.

        Args:
            timeout: Finite nonnegative remaining shutdown budget in seconds.

        Returns:
            `True` when no thread was started or all accept and peer threads
            have stopped before return.

        Raises:
            ValueError: The timeout is not finite and nonnegative.
        """
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError('probe join timeout must be finite and nonnegative')
        deadline = monotonic() + timeout
        thread = self._thread
        if thread is None:
            return True
        if thread is threading.current_thread():
            return False
        thread.join(timeout)
        if thread.is_alive():
            return False
        with self._lock:
            peer_threads = tuple(self._peer_threads)
        for peer_thread in peer_threads:
            if peer_thread is not threading.current_thread():
                peer_thread.join(max(0.0, deadline - monotonic()))
        with self._lock:
            self._peer_threads.difference_update(
                peer_thread for peer_thread in peer_threads if not peer_thread.is_alive()
            )
            return not self._peer_threads

    def start(self) -> None:
        """Start exactly one owned server thread.

        Raises:
            RuntimeError: The listener is closed or its thread was already
                started.
        """
        with self._lock:
            if self._closing:
                raise RuntimeError('probe listener is closed')
            if self._thread is not None:
                raise RuntimeError('probe listener is already started')
            thread = threading.Thread(target=self._serve, name='sg-event-probe', daemon=True)
            self._thread = thread
        try:
            thread.start()
        except BaseException:
            with self._lock:
                if self._thread is thread:
                    self._thread = None
            raise


@dataclass(frozen=True)
class ProbeSnapshot:
    """Contain one complete immutable supervisor readiness projection."""

    generated_at: float
    ready: bool
    complete: bool
    conditions: tuple[str, ...]
    shutting_down: bool
    halted: bool

    def __post_init__(self) -> None:
        """Validate one body-free immutable snapshot.

        Raises:
            TypeError: `conditions` is not exactly a tuple.
            ValueError: A condition is not a nonempty string, or `generated_at`
                is not a finite nonnegative integer or float.
        """
        _validate_time(self.generated_at, 'probe generation')
        if type(self.conditions) is not tuple:
            raise TypeError('probe conditions must be a tuple')
        if any(type(condition) is not str or not condition for condition in self.conditions):
            raise ValueError('probe conditions must be nonempty text')


class SupervisorProgress:
    """Own the short-lock foreground heartbeat and readiness snapshot."""

    def __init__(
        self,
        *,
        supervisor_stale_after: float,
        readiness_snapshot_stale_after: float,
    ) -> None:
        """Configure freshness gates for one supervisor.

        Args:
            supervisor_stale_after: Finite positive maximum foreground
                heartbeat age in seconds.
            readiness_snapshot_stale_after: Finite positive maximum complete
                snapshot age in seconds.

        Raises:
            ValueError: Either threshold is not finite and positive.
        """
        if (
            type(supervisor_stale_after) not in {float, int}
            or not isfinite(supervisor_stale_after)
            or supervisor_stale_after <= 0
            or type(readiness_snapshot_stale_after) not in {float, int}
            or not isfinite(readiness_snapshot_stale_after)
            or readiness_snapshot_stale_after <= 0
        ):
            raise ValueError('probe stale thresholds must be positive')
        self._heartbeat_at: float | None = None
        self._halted = False
        self._lock = threading.Lock()
        self._readiness_snapshot_stale_after = readiness_snapshot_stale_after
        self._snapshot: ProbeSnapshot | None = None
        self._shutting_down = False
        self._supervisor_stale_after = supervisor_stale_after

    def advance(self, *, now: float) -> None:
        """Record semantic foreground progress.

        Args:
            now: Finite nonnegative supervisor monotonic time in seconds.

        Raises:
            ValueError: The observation time is not finite and nonnegative.
        """
        _validate_time(now, 'supervisor progress')
        with self._lock:
            self._heartbeat_at = now

    def publish(self, snapshot: ProbeSnapshot) -> None:
        """Atomically replace the immutable readiness snapshot.

        Args:
            snapshot: Complete in-memory readiness projection.
        """
        with self._lock:
            self._snapshot = snapshot
            self._halted = snapshot.halted
            self._shutting_down = snapshot.shutting_down

    def result(self, *, now: float) -> ProbeResult:
        """Apply liveness and snapshot freshness gates without I/O.

        Args:
            now: Finite nonnegative monotonic observation time in seconds.

        Returns:
            One immutable request-time probe result.

        Raises:
            ValueError: The observation time is not finite and nonnegative.
        """
        _validate_time(now, 'probe observation')
        with self._lock:
            heartbeat_at = self._heartbeat_at
            halted = self._halted
            shutting_down = self._shutting_down
            snapshot = self._snapshot
        liveness = (
            heartbeat_at is not None
            and now - heartbeat_at < self._supervisor_stale_after
            and not halted
        )
        if snapshot is None:
            return ProbeResult(
                liveness=liveness,
                readiness=False,
                complete=False,
                conditions=(),
                generated_at=None,
                snapshot_age_seconds=None,
            )
        snapshot_age = max(0.0, now - snapshot.generated_at)
        liveness = liveness and not snapshot.halted
        readiness = (
            liveness
            and snapshot.ready
            and snapshot.complete
            and not shutting_down
            and not snapshot.shutting_down
            and not halted
            and not snapshot.halted
            and snapshot_age < self._readiness_snapshot_stale_after
        )
        return ProbeResult(
            liveness=liveness,
            readiness=readiness,
            complete=snapshot.complete,
            conditions=snapshot.conditions,
            generated_at=snapshot.generated_at,
            snapshot_age_seconds=snapshot_age,
        )

    def set_halted(self) -> None:
        """Fail liveness and readiness closed after a terminal halt."""
        with self._lock:
            self._halted = True

    def set_shutting_down(self) -> None:
        """Fail readiness closed immediately during graceful shutdown."""
        with self._lock:
            self._shutting_down = True


class _BadRequestError(ValueError):
    """Report one malformed or oversized HTTP request."""

    def __init__(self, *, is_head: bool = False) -> None:
        """Record whether a parseable request line selected `HEAD`."""
        self._is_head = is_head


def _is_head_request(buffer: bytearray) -> bool:
    """Return whether a complete valid request line selects `HEAD`."""
    request_line_end = buffer.find(b'\r\n')
    if request_line_end < 0:
        return False
    try:
        method, _path, version = bytes(buffer[:request_line_end]).decode('ascii').split(' ')
    except (UnicodeDecodeError, ValueError):
        return False
    return method == 'HEAD' and version in {'HTTP/1.0', 'HTTP/1.1'}


def _parse_request(request_line: bytes, header_block: bytes) -> tuple[str, str]:
    """Validate one complete bounded HTTP request head."""
    try:
        method, path, version = request_line.decode('ascii').split(' ')
    except (UnicodeDecodeError, ValueError) as error:
        raise _BadRequestError from error
    is_head = method == 'HEAD' and version in {'HTTP/1.0', 'HTTP/1.1'}
    try:
        header_lines = header_block.split(b'\r\n') if header_block else []
        headers_valid = all(
            name and separator == b':'
            for name, separator, _value in (line.partition(b':') for line in header_lines)
        )
        header_block.decode('ascii')
    except (UnicodeDecodeError, ValueError) as error:
        raise _BadRequestError(is_head=is_head) from error
    if version not in {'HTTP/1.0', 'HTTP/1.1'} or not headers_valid:
        raise _BadRequestError(is_head=is_head)
    return method, path


def _request_boundaries(buffer: bytearray) -> tuple[int, int] | None:
    """Return complete bounded request section boundaries when available."""
    request_line_end = buffer.find(b'\r\n')
    if request_line_end < 0:
        if len(buffer) >= _MAX_REQUEST_LINE_BYTES:
            raise _BadRequestError
        return None
    if request_line_end + 2 > _MAX_REQUEST_LINE_BYTES:
        raise _BadRequestError
    header_end = buffer.find(b'\r\n\r\n', request_line_end)
    if header_end < 0:
        if len(buffer) - (request_line_end + 2) >= _MAX_HEADER_BYTES:
            raise _BadRequestError
        return None
    header_bytes = header_end + 4 - (request_line_end + 2)
    if header_bytes > _MAX_HEADER_BYTES:
        raise _BadRequestError
    return request_line_end, header_end


def _set_peer_timeout(peer: socket.socket, *, deadline: float) -> None:
    """Apply the positive time remaining in one accepted-peer budget."""
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise TimeoutError
    peer.settimeout(remaining)


def _read_request(peer: socket.socket, *, deadline: float) -> tuple[str, str]:
    """Read and validate one bounded HTTP request."""
    buffer = bytearray()
    maximum_bytes = _MAX_REQUEST_LINE_BYTES + _MAX_HEADER_BYTES
    while True:
        try:
            boundaries = _request_boundaries(buffer)
        except _BadRequestError as error:
            raise _BadRequestError(is_head=_is_head_request(buffer)) from error
        if boundaries is not None:
            break
        remaining = maximum_bytes + 1 - len(buffer)
        if remaining <= 0:
            raise _BadRequestError(is_head=_is_head_request(buffer))
        try:
            _set_peer_timeout(peer, deadline=deadline)
            chunk = peer.recv(min(4_096, remaining))
        except TimeoutError as error:
            raise _BadRequestError(is_head=_is_head_request(buffer)) from error
        if not chunk:
            raise _BadRequestError(is_head=_is_head_request(buffer))
        buffer.extend(chunk)
    request_line_end, header_end = boundaries
    request_line = bytes(buffer[:request_line_end])
    header_block = bytes(buffer[request_line_end + 2 : header_end])
    return _parse_request(request_line, header_block)


def _validate_time(value: float, name: str) -> None:
    """Reject a non-finite or negative monotonic time value."""
    if type(value) not in {float, int} or not isfinite(value) or value < 0:
        raise ValueError(f'{name} time must be finite and nonnegative')
