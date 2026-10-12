"""Closed local operator control protocol."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import errno
import json
import math
import os
import socket
import stat
import struct
import time
import uuid
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, TypeAlias, cast

from ._logging import SENSITIVE_NAMES
from ._platform import (
    FILE_MODE,
    PeerIdentity,
    SecurityError,
    local_socket_path,
    local_socket_path_candidate,
    peer_identity,
    validate_secure_directory,
)

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType
    from typing import Self

_ALLOWED_COMMANDS = frozenset(
    {
        'acknowledge-ordering-anomaly',
        'adopt-source-stream',
        'park',
        'probe',
        'replay',
        'reset-group-to-latest',
        'reset-worker-breaker',
        'restart-event',
        'retry',
        'skip-callback',
        'status',
        'unpark',
    }
)

_MAX_MESSAGE_BYTES = 1_048_576
_CONTROL_IDENTITY = ('supervisor',)
_PROTOCOL_VERSION = 1
REQUEST_TIMEOUT_SECS = 5.0
_EVENT_BODY_KEYS = frozenset(
    {
        'event',
        'event_body',
        'eventlogentry',
        'event_log_entry',
    }
)
_LENGTH_BYTES = 4
_UNIX_STREAM_DISCONNECT_ERRNOS = frozenset(
    {
        errno.ECONNABORTED,
        errno.ECONNRESET,
        errno.ENOTCONN,
        errno.EPIPE,
        errno.ESHUTDOWN,
    }
)

_JsonValue: TypeAlias = (
    bool | float | int | list['_JsonValue'] | dict[str, '_JsonValue'] | str | None
)
_ControlHandler: TypeAlias = Callable[['ControlRequest', PeerIdentity], Mapping[str, object]]


class ControlClient:
    """Send correlated requests to one running local supervisor."""

    def __init__(
        self,
        path: Path,
        *,
        timeout: float = REQUEST_TIMEOUT_SECS,
    ) -> None:
        """Configure one bounded client.

        Args:
            path: Existing supervisor control socket.
            timeout: Complete connect and response deadline in positive
                seconds.

        Raises:
            ValueError: The timeout is not positive.
        """
        if timeout <= 0:
            raise ValueError('control timeout must be positive')
        self._timeout = timeout
        self._path = path

    @classmethod
    def for_state_dir(
        cls,
        state_dir: Path,
        *,
        timeout: float = REQUEST_TIMEOUT_SECS,
    ) -> Self:
        """Build a client for one configured state directory.

        Args:
            state_dir: Minimal control-plane state directory.
            timeout: Complete connect and response deadline in positive
                seconds.

        Returns:
            Client using the ordinary or hashed supervisor path.

        Raises:
            ValueError: The timeout is not positive.
        """
        return cls(_control_socket_path(state_dir), timeout=timeout)

    def request(
        self,
        command: str,
        arguments: Mapping[str, object] | None = None,
        *,
        request_id: str | None = None,
    ) -> Mapping[str, object]:
        """Send one command and return its structured result.

        Args:
            command: One command from the closed operator surface.
            arguments: Body-free command arguments.
            request_id: Optional caller-supplied stable correlation id.

        Returns:
            Recursively JSON-compatible result object.

        Raises:
            ControlCommandError: If the running supervisor rejects the command.
            ControlProtocolError: If either envelope violates the protocol.
            ControlUnavailableError: If the socket is absent, closes, or times
                out.
            SecurityError: If the socket parent is unsafe or the kernel peer
                credentials do not identify the effective user.
        """
        request = ControlRequest(
            _version=_PROTOCOL_VERSION,
            request_id=uuid.uuid4().hex if request_id is None else request_id,
            command=command,
            arguments={} if arguments is None else arguments,
        )
        frame = _encode_request(request)
        try:
            validate_secure_directory(self._path.parent)
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                deadline_at = time.monotonic() + self._timeout
                sock.settimeout(_remaining_timeout(deadline_at))
                sock.connect(os.fspath(self._path))
                _authenticate_peer(sock)
                sock.settimeout(_remaining_timeout(deadline_at))
                try:
                    sock.sendall(frame)
                except OSError as error:
                    if error.errno in _UNIX_STREAM_DISCONNECT_ERRNOS:
                        raise ControlUnavailableError(
                            'control peer closed before response'
                        ) from error
                    raise
                response = _decode_response(_read_frame(sock, deadline_at=deadline_at))
        except ControlProtocolError:
            raise
        except ControlUnavailableError:
            raise
        except TimeoutError as error:
            raise ControlUnavailableError('control request timed out') from error
        except OSError as error:
            raise ControlUnavailableError(f'control IPC is unavailable: {error}') from error
        if response._request_id != request.request_id:
            raise ControlProtocolError('control response request id does not match request')
        if not response._ok:
            raise ControlCommandError(response._error or 'control command failed')
        if response._result is None:
            raise ControlProtocolError('successful control response has no result')
        return MappingProxyType(dict(response._result))


class ControlCommandError(RuntimeError):
    """Report a command that the running supervisor rejected."""


class ControlProtocolError(ValueError):
    """Report malformed, unsupported, or unsafe operator control data."""


@dataclass(frozen=True)
class ControlRequest:
    """Contain one closed body-free operator request."""

    _version: int
    request_id: str
    command: str
    arguments: Mapping[str, object]

    def __post_init__(self) -> None:
        """Validate request fields and protect the copied outer argument mapping.

        Raises:
            ControlProtocolError: The protocol version is not an exact integer,
                the request id is empty, the command is unsupported, or
                arguments contain non-string keys, prohibited event-body or
                secret fields, non-finite numbers, or unsupported JSON values.
        """
        if type(self._version) is not int:
            raise ControlProtocolError('control protocol version must be an integer')
        if not self.request_id:
            raise ControlProtocolError('control request id must not be empty')
        if self.command not in _ALLOWED_COMMANDS:
            raise ControlProtocolError(f'unsupported control command {self.command!r}')
        arguments = _validate_mapping(self.arguments, context='control arguments')
        object.__setattr__(self, 'arguments', MappingProxyType(arguments))


class ControlUnavailableError(ConnectionError):
    """Report unavailable, timed-out, or closed local control IPC."""


@dataclass(frozen=True)
class _ControlResponse:
    """Contain one correlated supervisor response."""

    _version: int
    _request_id: str
    _ok: bool
    _result: Mapping[str, object] | None
    _error: str | None


class ControlServer:
    """Own one mode-0600 foreground local supervisor endpoint.

    Context entry opens the listener; context exit closes it and removes its
    socket path. `serve_once` accepts and handles one connection synchronously
    in the calling thread.
    """

    def __init__(
        self,
        state_dir: Path,
        handler: _ControlHandler,
    ) -> None:
        """Configure one unopened control endpoint.

        Args:
            state_dir: Secure daemon state directory.
            handler: Foreground command dispatcher receiving trusted peer
                identity.
        """
        self._foreground_progress: Callable[[], None] | None = None
        self._handler = handler
        self._listener: socket.socket | None = None
        self._state_dir = state_dir
        self._path = _control_socket_path(state_dir)

    def __enter__(self) -> Self:
        """Open and return the server.

        Returns:
            This server while it owns the socket path.
        """
        self._open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the endpoint when leaving a context."""
        self._close()

    def _close(self) -> None:
        """Close the listener and exact owned socket path."""
        listener = self._listener
        self._listener = None
        if listener is not None:
            listener.close()
        try:
            metadata = self._path.lstat()
        except FileNotFoundError:
            return
        if not stat.S_ISSOCK(metadata.st_mode) or metadata.st_uid != os.geteuid():
            raise SecurityError(f'control socket changed before cleanup: {self._path}')
        self._path.unlink()

    def _dispatch_request(
        self,
        request: ControlRequest,
        identity: PeerIdentity,
        foreground_progress: Callable[[], None] | None,
    ) -> _ControlResponse:
        """Run one validated handler and project its closed response."""
        try:
            result = self._handler(request, identity)
        except Exception as error:  # noqa: BLE001
            return _ControlResponse(
                _version=_PROTOCOL_VERSION,
                _request_id=request.request_id,
                _ok=False,
                _result=None,
                _error=str(error),
            )
        finally:
            if foreground_progress is not None:
                foreground_progress()
        return _ControlResponse(
            _version=_PROTOCOL_VERSION,
            _request_id=request.request_id,
            _ok=True,
            _result=result,
            _error=None,
        )

    def _open(self) -> None:
        """Bind and listen on the secure mode-0600 control path."""
        if self._listener is not None:
            return
        path = local_socket_path(self._state_dir, 'control', _CONTROL_IDENTITY)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            prior_umask = os.umask(0o177)
            try:
                listener.bind(os.fspath(path))
            finally:
                os.umask(prior_umask)
            path.chmod(FILE_MODE)
            listener.listen()
            listener.settimeout(0.1)
        except BaseException:
            listener.close()
            path.unlink(missing_ok=True)
            raise
        self._path = path
        self._listener = listener

    def _serve_peer(
        self,
        sock: socket.socket,
        foreground_progress: Callable[[], None] | None = None,
    ) -> None:
        """Authenticate and answer one request."""
        request_id = ''
        try:
            sock.settimeout(REQUEST_TIMEOUT_SECS)
            identity = peer_identity(sock)
            if identity.uid != os.geteuid():
                return
            body = _read_frame(sock)
            try:
                fields = _decode_json(body)
                raw_request_id = fields.get('request_id')
                if isinstance(raw_request_id, str):
                    request_id = raw_request_id
                request = _validated_request(body)
                if foreground_progress is not None:
                    foreground_progress()
            except (ControlProtocolError, RecursionError, ValueError) as error:
                response = _ControlResponse(
                    _version=_PROTOCOL_VERSION,
                    _request_id=request_id,
                    _ok=False,
                    _result=None,
                    _error=str(error),
                )
                try:
                    frame = _encode_response(response)
                except ControlProtocolError:
                    frame = _encode_response(
                        _ControlResponse(
                            _version=_PROTOCOL_VERSION,
                            _request_id='',
                            _ok=False,
                            _result=None,
                            _error='control request is malformed',
                        )
                    )
            else:
                response = self._dispatch_request(request, identity, foreground_progress)
                frame = _encode_response(response)
            sock.sendall(frame)
            if foreground_progress is not None:
                foreground_progress()
        except (
            ControlProtocolError,
            ControlUnavailableError,
            OSError,
            SecurityError,
        ):
            return
        finally:
            sock.close()

    def serve_once(self, timeout: float) -> bool:
        """Serve at most one request synchronously on the calling thread.

        Args:
            timeout: Finite nonnegative seconds to wait for a new connection.

        Returns:
            `True` after accepting one connection, otherwise `False` on
            timeout.

        Raises:
            ValueError: The timeout is not finite and nonnegative.
            RuntimeError: The server is not open.
        """
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError('control wait timeout must be finite and nonnegative')
        listener = self._listener
        if listener is None:
            raise RuntimeError('control server is not open')
        listener.settimeout(timeout)
        try:
            try:
                peer, _address = listener.accept()
            except (BlockingIOError, TimeoutError):
                return False
        finally:
            listener.settimeout(0.1)
        self._serve_peer(peer, self._foreground_progress)
        return True

    def set_foreground_progress(
        self,
        callback: Callable[[], None] | None,
    ) -> None:
        """Bind semantic progress used only by synchronous `serve_once`."""
        self._foreground_progress = callback


def _authenticate_peer(sock: socket.socket) -> None:
    """Reject a control endpoint without matching kernel-authenticated ownership."""
    if peer_identity(sock).uid != os.geteuid():
        raise SecurityError('control peer has the wrong owner')


def _control_socket_path(state_dir: Path) -> Path:
    """Return the deterministic control socket path without mutating it."""
    return local_socket_path_candidate(state_dir, 'control', _CONTROL_IDENTITY)


def _decode_json(body: bytes) -> dict[str, object]:
    """Decode one JSON object without executable serialization."""
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ControlProtocolError('control message is not valid JSON') from error
    if not isinstance(value, dict):
        raise ControlProtocolError('control message must be a JSON object')
    mapping = cast('dict[object, object]', value)
    if not all(isinstance(key, str) for key in mapping):
        raise ControlProtocolError('control message must be a JSON object')
    return cast('dict[str, object]', mapping)


def _decode_request(body: bytes) -> ControlRequest:
    """Decode one closed request envelope."""
    fields = _decode_json(body)
    if set(fields) != {'arguments', 'command', 'request_id', 'version'}:
        raise ControlProtocolError('control request fields are invalid')
    request_id = fields['request_id']
    command = fields['command']
    arguments = fields['arguments']
    version = fields['version']
    if not isinstance(request_id, str) or not isinstance(command, str):
        raise ControlProtocolError('control request identity is invalid')
    if type(version) is not int:
        raise ControlProtocolError('control request version is invalid')
    if not isinstance(arguments, dict):
        raise ControlProtocolError('control request arguments must be an object')
    return ControlRequest(
        _version=version,
        request_id=request_id,
        command=command,
        arguments=cast('dict[str, object]', arguments),
    )


def _decode_response(body: bytes) -> _ControlResponse:
    """Decode one closed correlated response envelope."""
    fields = _decode_json(body)
    if set(fields) != {'error', 'ok', 'request_id', 'result', 'version'}:
        raise ControlProtocolError('control response fields are invalid')
    version = fields['version']
    request_id = fields['request_id']
    ok = fields['ok']
    result = fields['result']
    error = fields['error']
    if type(version) is not int or version != _PROTOCOL_VERSION:
        raise ControlProtocolError(f'unsupported control protocol version {version!r}')
    if not isinstance(request_id, str) or not request_id:
        raise ControlProtocolError('control response request id is invalid')
    if type(ok) is not bool:
        raise ControlProtocolError('control response status is invalid')
    if result is not None and not isinstance(result, dict):
        raise ControlProtocolError('control response result must be an object')
    if error is not None and not isinstance(error, str):
        raise ControlProtocolError('control response error must be text')
    if ok == (error is not None):
        raise ControlProtocolError('control response outcome is inconsistent')
    return _ControlResponse(
        _version=version,
        _request_id=request_id,
        _ok=ok,
        _result=None if result is None else cast('dict[str, object]', result),
        _error=error,
    )


def _remaining_timeout(deadline_at: float) -> float:
    """Return the positive time left before one control deadline."""
    remaining = deadline_at - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('control peer exceeded the request deadline')
    return remaining


def _read_exact(sock: socket.socket, size: int, deadline_at: float) -> bytes:
    """Read one exact bounded byte count before a monotonic deadline."""
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        sock.settimeout(_remaining_timeout(deadline_at))
        chunk = sock.recv(remaining)
        if not chunk:
            raise ControlUnavailableError('control peer closed before response')
        chunks.append(chunk)
        remaining -= len(chunk)
    return b''.join(chunks)


def _read_frame(
    sock: socket.socket,
    *,
    deadline_at: float | None = None,
) -> bytes:
    """Read one frame under one deadline and restore the prior timeout."""
    if deadline_at is None:
        deadline_at = time.monotonic() + REQUEST_TIMEOUT_SECS
    prior_timeout = sock.gettimeout()
    try:
        prefix = _read_exact(sock, _LENGTH_BYTES, deadline_at)
        (length,) = struct.unpack('!I', prefix)
        if length <= 0:
            raise ControlProtocolError('control frame length must be positive')
        if length > _MAX_MESSAGE_BYTES:
            raise ControlProtocolError('control frame length exceeds configured maximum')
        return _read_exact(sock, length, deadline_at)
    finally:
        sock.settimeout(prior_timeout)


def _validate_mapping(
    value: Mapping[str, object] | Mapping[object, object],
    *,
    context: str,
) -> dict[str, _JsonValue]:
    """Validate one body-free string-keyed JSON mapping."""
    result: dict[str, _JsonValue] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ControlProtocolError(f'{context} must use string keys')
        normalized = key.casefold().replace('-', '_')
        if normalized in _EVENT_BODY_KEYS:
            raise ControlProtocolError(f'{context} contains an EventLogEntry body')
        if normalized in SENSITIVE_NAMES:
            raise ControlProtocolError(f'{context} contains secret material')
        result[key] = _validate_value(item, context=f'{context}.{key}')
    return result


def _validate_value(value: object, *, context: str) -> _JsonValue:
    """Validate and copy one recursively body-free JSON value."""
    if value is None or type(value) in {bool, int, str}:
        return cast('bool | int | str | None', value)
    if type(value) is float:
        if not math.isfinite(value):
            raise ControlProtocolError(f'{context} contains a non-finite number')
        return value
    if isinstance(value, Mapping):
        return _validate_mapping(cast('Mapping[object, object]', value), context=context)
    if isinstance(value, list | tuple):
        items = cast('Collection[object]', value)
        return [_validate_value(item, context=context) for item in items]
    raise ControlProtocolError(f'{context} contains unsupported {type(value).__name__}')


def _validated_request(body: bytes) -> ControlRequest:
    """Decode one request and require the local protocol version."""
    request = _decode_request(body)
    if request._version != _PROTOCOL_VERSION:
        raise ControlProtocolError(f'unsupported control protocol version {request._version}')
    return request


def encode_json_body(value: Mapping[str, object]) -> bytes:
    """Encode one JSON object with the control protocol's exact settings.

    Args:
        value: JSON-compatible object fields, copied without mutating the
            mapping.
            Mapping iteration order is preserved and non-finite numbers are
            rejected.

    Returns:
        Compact UTF-8 JSON bytes with non-ASCII characters escaped. The body
        has no length prefix or size limit; the control frame encoder enforces
        its own limit.

    Raises:
        ControlProtocolError: Converting or encoding the mapping raises
            `TypeError` or `ValueError`, including unsupported values and
            non-finite numbers.
    """
    try:
        return json.dumps(
            dict(value),
            allow_nan=False,
            ensure_ascii=True,
            separators=(',', ':'),
        ).encode()
    except (TypeError, ValueError) as error:
        raise ControlProtocolError('control message is not JSON-compatible') from error


def _encode_frame(value: Mapping[str, object]) -> bytes:
    """Encode one bounded length-prefixed JSON object."""
    body = encode_json_body(value)
    if not body or len(body) > _MAX_MESSAGE_BYTES:
        raise ControlProtocolError(
            f'control message exceeds maximum of {_MAX_MESSAGE_BYTES} bytes'
        )
    return struct.pack('!I', len(body)) + body


def _encode_request(request: ControlRequest) -> bytes:
    """Encode one validated request."""
    return _encode_frame(
        {
            'version': request._version,
            'request_id': request.request_id,
            'command': request.command,
            'arguments': dict(request.arguments),
        }
    )


def _encode_response(response: _ControlResponse) -> bytes:
    """Encode one validated response."""
    return _encode_frame(
        {
            'version': response._version,
            'request_id': response._request_id,
            'ok': response._ok,
            'result': None if response._result is None else dict(response._result),
            'error': response._error,
        }
    )
