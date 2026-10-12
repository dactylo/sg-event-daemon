from __future__ import annotations

import errno
import json
import os
import socket
import stat
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, cast
from unittest.mock import Mock

import pytest

import sg_event_daemon._cli as cli_module
import sg_event_daemon._control as control_module
import sg_event_daemon._platform as platform_adapter
from sg_event_daemon._control import (
    _MAX_MESSAGE_BYTES,
    _PROTOCOL_VERSION,
    ControlClient,
    ControlProtocolError,
    ControlRequest,
    ControlServer,
    ControlUnavailableError,
    _control_socket_path,
    encode_json_body,
)
from sg_event_daemon._platform import PeerIdentity, SecurityError
from sg_event_daemon._probe import ProbeServer, ProbeSnapshot, SupervisorProgress
from sg_event_daemon._supervisor import Supervisor

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
    from pathlib import Path
    from types import TracebackType
    from typing import Self


class _FrameSocket:
    def __init__(self, chunks: list[bytes], *, response_timeout: float) -> None:
        self._chunks = iter(chunks)
        self._response_timeout = response_timeout
        self._timeout: float | None = response_timeout
        self._sent = bytearray()

    def gettimeout(self) -> float | None:
        return self._timeout

    def recv(self, size: int) -> bytes:
        chunk = next(self._chunks)
        assert len(chunk) <= size
        return chunk

    def sendall(self, data: bytes) -> None:
        if self._timeout != self._response_timeout:
            raise TimeoutError('response inherited the residual read timeout')
        self._sent.extend(data)

    def settimeout(self, timeout: float | None) -> None:
        self._timeout = timeout


class _ScriptedClientSocket:
    def __init__(
        self,
        chunks: list[bytes],
        *,
        elapsed_by_operation: tuple[float, ...],
    ) -> None:
        self._chunks = iter(chunks)
        self._elapsed_by_operation = iter(elapsed_by_operation)
        self._now = 10.0
        self._recv_count = 0
        self._timeout: float | None = None
        self._operations: list[tuple[str, float]] = []
        self._timeouts: list[float | None] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        return None

    def _record_operation(self, operation: str) -> None:
        timeout = self._timeout
        if timeout is None or timeout <= 0:
            raise AssertionError(f'{operation} ran without a positive timeout')
        self._operations.append((operation, timeout))
        self._now += next(self._elapsed_by_operation)

    def connect(self, _path: str) -> None:
        self._record_operation('connect')

    def gettimeout(self) -> float | None:
        return self._timeout

    def monotonic(self) -> float:
        return self._now

    def recv(self, size: int) -> bytes:
        operation = 'recv-prefix' if self._recv_count == 0 else 'recv-body'
        self._recv_count += 1
        self._record_operation(operation)
        chunk = next(self._chunks, None)
        if chunk is None:
            raise TimeoutError('scripted socket receive timed out')
        assert len(chunk) <= size
        return chunk

    def sendall(self, _data: bytes) -> None:
        self._record_operation('sendall')

    def settimeout(self, timeout: float | None) -> None:
        self._timeout = timeout
        self._timeouts.append(timeout)


def _frozen_probe_supervisor(
    clock: Callable[[], float],
) -> tuple[Supervisor, SupervisorProgress, threading.Event, threading.Event]:
    progress = SupervisorProgress(
        supervisor_stale_after=0.25,
        readiness_snapshot_stale_after=1.0,
    )
    progress.advance(now=clock())
    progress.publish(
        ProbeSnapshot(
            generated_at=clock(),
            ready=True,
            complete=True,
            conditions=('healthy',),
            shutting_down=False,
            halted=False,
        )
    )
    supervisor = object.__new__(Supervisor)
    vars(supervisor)['_clock'] = clock
    vars(supervisor)['_progress'] = progress
    real_probe_mapping = cast(
        'Callable[[], Mapping[str, object]]',
        vars(Supervisor)['probe_mapping'].__get__(supervisor),
    )
    handler_entered = threading.Event()
    release_handler = threading.Event()

    def frozen_probe_mapping() -> Mapping[str, object]:
        handler_entered.set()
        if not release_handler.wait(2.0):
            raise TimeoutError('test did not release frozen probe handler')
        return real_probe_mapping()

    vars(supervisor)['probe_mapping'] = frozen_probe_mapping
    return supervisor, progress, handler_entered, release_handler


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _probe_http_response(probe: ProbeServer) -> bytes:
    listener = cast('socket.socket', vars(probe)['_listener'])
    with socket.create_connection(listener.getsockname(), timeout=1.0) as client:
        client.settimeout(1.0)
        client.sendall(b'GET /livez HTTP/1.1\r\n\r\n')
        response = bytearray()
        while chunk := client.recv(4_096):
            response.extend(chunk)
    return bytes(response)


def _read_response(sock: socket.socket) -> dict[str, object]:
    prefix = sock.recv(4)
    assert len(prefix) == 4
    length = struct.unpack('!I', prefix)[0]
    body = bytearray()
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        assert chunk
        body.extend(chunk)
    response = json.loads(body)
    assert isinstance(response, dict)
    return cast('dict[str, object]', response)


def _request_probe(
    state_dir: Path,
    done: threading.Event,
    errors: list[ControlUnavailableError],
) -> None:
    try:
        ControlClient.for_state_dir(state_dir, timeout=0.05).request('probe')
    except ControlUnavailableError as error:
        errors.append(error)
    finally:
        done.set()


def _run_server(server: ControlServer, stop: threading.Event) -> None:
    while not stop.is_set():
        server.serve_once(0.1)


def _standalone_socket_path(tmp_path: Path) -> Path:
    path = _control_socket_path(tmp_path)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    return path


@pytest.fixture
def running_server(tmp_path: Path) -> Iterator[tuple[ControlServer, Path]]:
    state_dir = tmp_path / 'state'

    def handler(
        request: ControlRequest,
        identity: PeerIdentity,
    ) -> Mapping[str, object]:
        return {
            'command': request.command,
            'arguments': dict(request.arguments),
            'peer_pid': identity.pid,
        }

    stop = threading.Event()
    with ControlServer(state_dir, handler) as server:
        thread = threading.Thread(target=_run_server, args=(server, stop))
        try:
            thread.start()
            yield server, state_dir
        finally:
            stop.set()
            if thread.ident is not None:
                thread.join(2)
                assert not thread.is_alive()


def test_client_correlates_response_request_id(tmp_path: Path) -> None:
    path = _standalone_socket_path(tmp_path)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(os.fspath(path))
    listener.listen()
    listener.settimeout(1.0)

    def reply_with_wrong_id() -> None:
        try:
            peer, _address = listener.accept()
        except TimeoutError:
            return
        with peer:
            peer.settimeout(1.0)
            control_module._read_frame(peer, deadline_at=time.monotonic() + 1.0)
            body = json.dumps(
                {
                    'version': _PROTOCOL_VERSION,
                    'request_id': 'wrong-id',
                    'ok': True,
                    'result': {},
                    'error': None,
                },
                separators=(',', ':'),
            ).encode()
            peer.sendall(struct.pack('!I', len(body)) + body)

    with ThreadPoolExecutor(max_workers=1) as executor:
        response = executor.submit(reply_with_wrong_id)
        try:
            with pytest.raises(ControlProtocolError, match='request id'):
                ControlClient(path).request('status', request_id='expected-id')
        finally:
            try:
                response.result(timeout=3.0)
            finally:
                listener.close()
                path.unlink(missing_ok=True)


def test_client_rejects_oversized_outbound_message(tmp_path: Path) -> None:
    path = tmp_path / 'missing.sock'
    client = ControlClient(path)

    with pytest.raises(ControlProtocolError, match='maximum'):
        client.request('park', {'reason': 'x' * _MAX_MESSAGE_BYTES})


@pytest.mark.parametrize('unsafe_parent', ['mode', 'symlink', 'foreign'])
def test_client_rejects_unsafe_parent_before_connecting(
    unsafe_parent: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    parent = tmp_path / 'endpoint'
    parent.mkdir(mode=0o700)
    if unsafe_parent == 'mode':
        parent.chmod(0o755)
    elif unsafe_parent == 'symlink':
        link = tmp_path / 'link'
        link.symlink_to(parent, target_is_directory=True)
        parent = link
    else:
        monkeypatch.setattr(platform_adapter, '_effective_uid', lambda: os.geteuid() + 1)
    socket_factory = Mock(side_effect=AssertionError('connected before directory validation'))
    monkeypatch.setattr(control_module.socket, 'socket', socket_factory)
    before = parent.lstat()

    with pytest.raises(SecurityError):
        ControlClient(parent / 'control.sock').request('status')

    socket_factory.assert_not_called()
    assert parent.lstat() == before
    assert not list(parent.iterdir())


@pytest.mark.parametrize('peer_failure', ['foreign', 'unavailable'])
def test_client_authenticates_peer_before_sending_request(
    peer_failure: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = _standalone_socket_path(tmp_path)

    def rejected_identity(_socket: socket.socket) -> PeerIdentity:
        if peer_failure == 'unavailable':
            raise SecurityError('cannot read peer credentials')
        return PeerIdentity(uid=os.geteuid() + 1, gid=os.getegid(), pid=os.getpid())

    monkeypatch.setattr(control_module, 'peer_identity', rejected_identity)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind(os.fspath(path))
        listener.listen()
        listener.settimeout(1.0)
        try:
            with pytest.raises(SecurityError, match='peer'):
                ControlClient(path).request('status', {'reason': 'private operator text'})
            peer, _address = listener.accept()
            with peer:
                peer.settimeout(1.0)
                assert peer.recv(1) == b''
            assert path.exists()
        finally:
            path.unlink()


@pytest.mark.parametrize(
    ('error_number', 'message'),
    [
        pytest.param(errno.ECONNABORTED, 'peer closed', id='aborted'),
        pytest.param(errno.ECONNRESET, 'peer closed', id='reset'),
        pytest.param(errno.ENOTCONN, 'peer closed', id='not-connected'),
        pytest.param(errno.EPIPE, 'peer closed', id='broken-pipe'),
        pytest.param(errno.ESHUTDOWN, 'peer closed', id='shutdown'),
        pytest.param(errno.EIO, 'control IPC is unavailable', id='unrelated'),
    ],
)
def test_client_classifies_send_errors(
    error_number: int,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = _standalone_socket_path(tmp_path)
    real_socket = socket.socket
    listener = real_socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(os.fspath(path))
    listener.listen()

    class SendFailingSocket(real_socket):
        def sendall(self, _data: object, _flags: int = 0) -> None:
            raise OSError(error_number, os.strerror(error_number))

    monkeypatch.setattr(control_module.socket, 'socket', SendFailingSocket)
    try:
        with pytest.raises(ControlUnavailableError, match=message):
            ControlClient(path).request('status')
    finally:
        listener.close()
        path.unlink(missing_ok=True)


@pytest.mark.parametrize('nested', [{1: 'value'}, [{1: 'value'}], ({'child': {1: 'value'}},)])
def test_client_rejects_nested_nonstring_keys_before_socket_creation(
    nested: object,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    socket_factory = Mock(side_effect=AssertionError('socket created before validation'))
    monkeypatch.setattr(control_module.socket, 'socket', socket_factory)

    with pytest.raises(ControlProtocolError, match='string keys'):
        ControlClient(tmp_path / 'control.sock').request('status', {'nested': nested})

    socket_factory.assert_not_called()


def test_client_reports_eof_before_response(tmp_path: Path) -> None:
    path = _standalone_socket_path(tmp_path)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(os.fspath(path))
    listener.listen()
    listener.settimeout(1.0)

    def close_without_reply() -> None:
        try:
            peer, _address = listener.accept()
        except TimeoutError:
            return
        with peer:
            peer.settimeout(1.0)
            control_module._read_frame(peer, deadline_at=time.monotonic() + 1.0)

    with ThreadPoolExecutor(max_workers=1) as executor:
        response = executor.submit(close_without_reply)
        try:
            with pytest.raises(ControlUnavailableError, match='peer closed'):
                ControlClient(path).request('status')
        finally:
            try:
                response.result(timeout=3.0)
            finally:
                listener.close()
                path.unlink(missing_ok=True)


def test_client_reports_missing_parent_without_creating_it(tmp_path: Path) -> None:
    parent = tmp_path / 'missing'

    with pytest.raises(ControlUnavailableError, match='control IPC is unavailable'):
        ControlClient(parent / 'control.sock').request('status')

    assert not parent.exists()


def test_client_request_shares_one_deadline_across_socket_phases(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    body = b'{"version":1,"request_id":"request-1","ok":true,"result":{},"error":null}'
    response = struct.pack('!I', len(body)) + body
    sock = _ScriptedClientSocket(
        [response[:4], response[4:]],
        elapsed_by_operation=(1.0, 1.0, 1.0, 1.0),
    )

    def socket_factory(_family: int, _kind: int) -> _ScriptedClientSocket:
        return sock

    monkeypatch.setattr(control_module.time, 'monotonic', sock.monotonic)
    monkeypatch.setattr(control_module.socket, 'socket', socket_factory)
    monkeypatch.setattr(
        control_module,
        'peer_identity',
        Mock(return_value=PeerIdentity(uid=os.geteuid(), gid=os.getegid(), pid=os.getpid())),
    )

    client = ControlClient(tmp_path / 'control.sock', timeout=5.0)

    assert (
        client.request(
            'probe',
            {'check': 'readiness'},
            request_id='request-1',
        )
        == {}
    )
    assert sock._operations == [
        ('connect', 5.0),
        ('sendall', 4.0),
        ('recv-prefix', 3.0),
        ('recv-body', 2.0),
    ]
    assert sock._timeouts == [5.0, 4.0, 3.0, 2.0, 4.0]


@pytest.mark.parametrize(
    ('elapsed_by_operation', 'response_chunks', 'expected_activity'),
    [
        pytest.param(
            (5.0,),
            0,
            ([('connect', 5.0)], [5.0]),
            id='after-connect',
        ),
        pytest.param(
            (1.0, 4.0),
            0,
            (
                [('connect', 5.0), ('sendall', 4.0)],
                [5.0, 4.0, 4.0],
            ),
            id='after-send',
        ),
        pytest.param(
            (1.0, 1.0, 3.0),
            1,
            (
                [('connect', 5.0), ('sendall', 4.0), ('recv-prefix', 3.0)],
                [5.0, 4.0, 3.0, 4.0],
            ),
            id='after-prefix',
        ),
        pytest.param(
            (1.0, 1.0, 1.0, 2.0),
            2,
            (
                [
                    ('connect', 5.0),
                    ('sendall', 4.0),
                    ('recv-prefix', 3.0),
                    ('recv-body', 2.0),
                ],
                [5.0, 4.0, 3.0, 2.0, 4.0],
            ),
            id='after-partial-body',
        ),
    ],
)
def test_client_request_timeout_exhausts_after_each_socket_phase(
    elapsed_by_operation: tuple[float, ...],
    response_chunks: int,
    expected_activity: tuple[list[tuple[str, float]], list[float]],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    expected_operations, expected_timeouts = expected_activity
    body = b'{"version":1,"request_id":"request-1","ok":true,"result":{},"error":null}'
    response = struct.pack('!I', len(body)) + body
    chunks = [response[:4], response[4:5]][:response_chunks]
    sock = _ScriptedClientSocket(chunks, elapsed_by_operation=elapsed_by_operation)

    def socket_factory(_family: int, _kind: int) -> _ScriptedClientSocket:
        return sock

    monkeypatch.setattr(control_module.time, 'monotonic', sock.monotonic)
    monkeypatch.setattr(control_module.socket, 'socket', socket_factory)
    monkeypatch.setattr(
        control_module,
        'peer_identity',
        Mock(return_value=PeerIdentity(uid=os.geteuid(), gid=os.getegid(), pid=os.getpid())),
    )

    with pytest.raises(ControlUnavailableError) as caught:
        ControlClient(tmp_path / 'control.sock', timeout=5.0).request(
            'probe',
            {'check': 'readiness'},
            request_id='request-1',
        )

    assert str(caught.value) == 'control request timed out'
    assert isinstance(caught.value.__cause__, TimeoutError)
    assert str(caught.value.__cause__) == 'control peer exceeded the request deadline'
    assert sock._operations == expected_operations
    assert sock._timeouts == expected_timeouts


def test_client_times_out_waiting_for_response(tmp_path: Path) -> None:
    state_dir = tmp_path / 'state'
    release = threading.Event()

    def handler(
        _request: ControlRequest,
        _identity: PeerIdentity,
    ) -> Mapping[str, object]:
        release.wait(1)
        return {}

    stop = threading.Event()
    with ControlServer(state_dir, handler) as server:
        thread = threading.Thread(target=_run_server, args=(server, stop))
        try:
            thread.start()
            with pytest.raises(ControlUnavailableError, match='timed out'):
                ControlClient.for_state_dir(state_dir, timeout=0.01).request('status')
        finally:
            release.set()
            stop.set()
            if thread.ident is not None:
                thread.join(2)
                assert not thread.is_alive()


def test_concurrent_status_clients_receive_correlated_results(
    running_server: tuple[ControlServer, Path],
) -> None:
    _server, state_dir = running_server
    results: dict[str, Mapping[str, object]] = {}
    barrier = threading.Barrier(3)

    def request_status(request_id: str) -> None:
        barrier.wait()
        results[request_id] = ControlClient.for_state_dir(state_dir).request(
            'status',
            {'site': request_id},
            request_id=request_id,
        )

    first = threading.Thread(target=request_status, args=('first',))
    second = threading.Thread(target=request_status, args=('second',))
    first.start()
    second.start()
    barrier.wait()
    first.join(2)
    second.join(2)

    assert results['first']['arguments'] == {'site': 'first'}
    assert results['second']['arguments'] == {'site': 'second'}


def test_control_socket_path_delegates_to_platform_candidate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / 'state'
    sentinel = tmp_path / 'sentinel.sock'
    observed: list[tuple[Path, str, tuple[str, ...]]] = []

    def candidate(
        received_state_dir: Path,
        kind: str,
        identity: tuple[str, ...],
    ) -> Path:
        observed.append((received_state_dir, kind, identity))
        return sentinel

    monkeypatch.setattr(control_module, 'local_socket_path_candidate', candidate)

    assert _control_socket_path(state_dir) == sentinel
    assert observed == [(state_dir, 'control', ('supervisor',))]
    assert not state_dir.exists()


def test_probe_handler_freeze_cannot_refresh_http_liveness(tmp_path: Path) -> None:
    now = 0.0
    supervisor, progress, handler_entered, release_handler = _frozen_probe_supervisor(lambda: now)
    handle_request = cast(
        'Callable[[Supervisor, ControlRequest, PeerIdentity], Mapping[str, object]]',
        vars(cli_module)['_handle_supervisor_request'],
    )
    progress_events: list[float] = []

    def advance_progress() -> None:
        progress_events.append(now)
        progress.advance(now=now)

    def handle(request: ControlRequest, peer: PeerIdentity) -> Mapping[str, object]:
        return handle_request(supervisor, request, peer)

    probe = ProbeServer('127.0.0.1', 0, supervisor.probe_result)
    client_done = threading.Event()
    client_errors: list[ControlUnavailableError] = []
    control_thread: threading.Thread | None = None
    client_thread: threading.Thread | None = None
    try:
        probe.start()
        with ControlServer(tmp_path / 'state', handle) as server:
            server.set_foreground_progress(advance_progress)
            try:
                client_thread = threading.Thread(
                    target=_request_probe,
                    args=(tmp_path / 'state', client_done, client_errors),
                )
                control_thread = threading.Thread(target=server.serve_once, args=(1.0,))
                client_thread.start()
                control_thread.start()
                assert handler_entered.wait(1.0)
                assert progress_events == [0.0]

                now = 0.25
                response = _probe_http_response(probe)

                assert response.startswith(b'HTTP/1.1 503 Service Unavailable\r\n')
                assert response.endswith(b'unavailable\n')
                assert progress_events == [0.0]
                assert client_done.wait(1.0)
                assert len(client_errors) == 1
                assert isinstance(client_errors[0], ControlUnavailableError)
                assert 'timed out' in str(client_errors[0])
            finally:
                release_handler.set()
                if client_thread is not None:
                    client_thread.join(2.0)
                    assert not client_thread.is_alive()
                if control_thread is not None:
                    control_thread.join(2.0)
                    assert not control_thread.is_alive()
    finally:
        probe.close()
        assert probe.join(timeout=2.0)


def test_read_frame_enforces_one_request_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = b'{"a": 1}'
    frame = struct.pack('!I', len(body)) + body
    server_sock, client_sock = socket.socketpair()
    monkeypatch.setattr(control_module, 'REQUEST_TIMEOUT_SECS', 0.2)
    read_frame = cast('Callable[[socket.socket], bytes]', vars(control_module)['_read_frame'])
    server_sock.settimeout(0.2)
    stop = threading.Event()

    def drip() -> None:
        try:
            for byte in frame:
                client_sock.sendall(bytes([byte]))
                if stop.wait(0.05):
                    return
        except OSError:
            return

    thread = threading.Thread(target=drip, daemon=True)
    started_at = time.monotonic()
    thread.start()
    try:
        with pytest.raises(TimeoutError):
            read_frame(server_sock)
        elapsed = time.monotonic() - started_at
    finally:
        stop.set()
        server_sock.close()
        client_sock.close()
        thread.join(1)

    assert not thread.is_alive()
    assert elapsed < 0.5


def test_read_frame_preserves_server_relative_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = b'{"a":1}'
    sock = _ScriptedClientSocket(
        [struct.pack('!I', len(body)), body],
        elapsed_by_operation=(1.0, 1.0),
    )
    sock.settimeout(2.0)
    monkeypatch.setattr(control_module, 'REQUEST_TIMEOUT_SECS', 5.0)
    monkeypatch.setattr(control_module.time, 'monotonic', sock.monotonic)
    read_frame = control_module._read_frame

    assert read_frame(cast('socket.socket', sock)) == body
    assert sock._operations == [('recv-prefix', 5.0), ('recv-body', 4.0)]
    assert sock._timeouts == [2.0, 5.0, 4.0, 2.0]


def test_read_frame_restores_timeout_after_malformed_length(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    readings = iter((10.0, 10.1))
    sock = _FrameSocket([struct.pack('!I', 0)], response_timeout=2.0)
    monkeypatch.setattr(control_module.time, 'monotonic', lambda: next(readings))
    read_frame = control_module._read_frame

    with pytest.raises(ControlProtocolError, match='length must be positive'):
        read_frame(cast('socket.socket', sock), deadline_at=15.0)
    sock.sendall(b'error response')

    assert sock._sent == b'error response'


def test_read_frame_restores_timeout_before_response_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = b'{"a": 1}'
    readings = iter((10.0, 10.1, 14.9))
    sock = _FrameSocket(
        [struct.pack('!I', len(body)), body],
        response_timeout=2.0,
    )
    monkeypatch.setattr(control_module.time, 'monotonic', lambda: next(readings))
    read_frame = control_module._read_frame

    assert read_frame(cast('socket.socket', sock), deadline_at=15.0) == body
    sock.sendall(b'response')

    assert sock._sent == b'response'


def test_request_accepts_internal_probe() -> None:
    request = ControlRequest(
        _version=_PROTOCOL_VERSION,
        request_id='probe-1',
        command='probe',
        arguments={},
    )

    assert request.command == 'probe'
    assert request.arguments == {}


@pytest.mark.parametrize(
    'arguments',
    [
        {'event': {'id': 123, 'type': 'EventLogEntry'}},
        {'payload': b'pickle-like-binary'},
        {'authorization': 'Bearer secret'},
    ],
)
def test_requests_reject_event_bodies_pickle_and_secret_fields(
    arguments: Mapping[str, object],
) -> None:
    with pytest.raises(ControlProtocolError, match='control arguments'):
        ControlRequest(
            _version=_PROTOCOL_VERSION,
            request_id='body-free',
            command='status',
            arguments=arguments,
        )


def test_server_creates_owner_only_socket_and_cleans_it_up(tmp_path: Path) -> None:
    state_dir = tmp_path / 'state'
    server = ControlServer(state_dir, lambda _request, _identity: {})

    with server:
        assert server._path.is_socket()
        assert _mode(server._path) == 0o600
        assert server._path.stat().st_uid == os.geteuid()

    assert not server._path.exists()


def test_server_creates_socket_privately_before_bind_returns(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    real_socket = socket.socket
    observed_modes: list[int] = []

    class ObservedSocket(real_socket):
        def bind(self, address: object) -> None:
            path = cast('str | bytes', address)
            super().bind(path)
            observed_modes.append(stat.S_IMODE(os.lstat(path).st_mode))

    monkeypatch.setattr(control_module.socket, 'socket', ObservedSocket)
    original_umask = os.umask(0)
    try:
        with ControlServer(tmp_path / 'state', lambda _request, _identity: {}):
            pass
    finally:
        os.umask(original_umask)

    assert observed_modes == [0o600]


@pytest.mark.parametrize('length', [0, _MAX_MESSAGE_BYTES + 1])
def test_server_rejects_invalid_frame_lengths(
    length: int,
    running_server: tuple[ControlServer, Path],
) -> None:
    server, _state_dir = running_server
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(1)
        client.connect(os.fspath(server._path))
        client.sendall(struct.pack('!I', length))

        assert client.recv(1) == b''


def test_server_rejects_peer_with_different_effective_uid(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def foreign_identity(_socket: socket.socket) -> PeerIdentity:
        return PeerIdentity(uid=os.geteuid() + 1, gid=os.getegid(), pid=303)

    monkeypatch.setattr(
        control_module,
        'peer_identity',
        foreign_identity,
    )
    stop = threading.Event()
    with ControlServer(tmp_path / 'state', lambda _request, _identity: {}) as server:
        thread = threading.Thread(target=_run_server, args=(server, stop))
        try:
            thread.start()
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(1.0)
                client.connect(os.fspath(server._path))
                assert client.recv(1) == b''
        finally:
            stop.set()
            if thread.ident is not None:
                thread.join(2)
                assert not thread.is_alive()


def test_server_rejects_version_mismatch(
    running_server: tuple[ControlServer, Path],
) -> None:
    server, _state_dir = running_server
    body = json.dumps(
        {
            'version': _PROTOCOL_VERSION + 1,
            'request_id': 'request-version',
            'command': 'status',
            'arguments': {},
        },
        separators=(',', ':'),
    ).encode()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(1)
        client.connect(os.fspath(server._path))
        client.sendall(struct.pack('!I', len(body)) + body)

        response_length = struct.unpack('!I', client.recv(4))[0]
        response = json.loads(client.recv(response_length))

    assert response == {
        'version': _PROTOCOL_VERSION,
        'request_id': 'request-version',
        'ok': False,
        'result': None,
        'error': f'unsupported control protocol version {_PROTOCOL_VERSION + 1}',
    }


@pytest.mark.parametrize(
    ('body', 'request_id'),
    [
        pytest.param(b'{', '', id='invalid-json'),
        pytest.param(
            (
                b'{"version":1,"request_id":"nested","command":"status",'
                b'"arguments":{"value":' + b'[' * 2_000 + b'0' + b']' * 2_000 + b'}}'
            ),
            None,
            id='excessive-nesting',
        ),
        pytest.param(
            b'{"version":1,"request_id":"structure","command":"status","arguments":[]}',
            'structure',
            id='wrong-structure',
        ),
        pytest.param(
            b'{"version":1,"request_id":"finite","command":"status","arguments":{"value":NaN}}',
            'finite',
            id='non-finite-number',
        ),
    ],
)
def test_server_reports_malformed_request_and_serves_next_peer(
    body: bytes,
    request_id: str | None,
    tmp_path: Path,
) -> None:
    if request_id is None:
        try:
            json.loads(body)
        except RecursionError:
            request_id = ''
        else:
            request_id = 'nested'

    state_dir = tmp_path / 'state'
    with ControlServer(
        state_dir,
        lambda _request, _identity: {'accepted': True},
    ) as server:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as malformed:
            malformed.settimeout(1)
            malformed.connect(os.fspath(server._path))
            malformed.sendall(struct.pack('!I', len(body)) + body)

            assert server.serve_once(0.0)
            response = _read_response(malformed)

        valid_body = b'{"version":1,"request_id":"valid","command":"status","arguments":{}}'
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as valid:
            valid.settimeout(1)
            valid.connect(os.fspath(server._path))
            valid.sendall(struct.pack('!I', len(valid_body)) + valid_body)

            assert server.serve_once(0.0)
            valid_response = _read_response(valid)

    assert response['request_id'] == request_id
    assert response['ok'] is False
    assert response['result'] is None
    assert isinstance(response['error'], str)
    assert response['error']
    assert valid_response == {
        'version': _PROTOCOL_VERSION,
        'request_id': 'valid',
        'ok': True,
        'result': {'accepted': True},
        'error': None,
    }


def test_server_reports_maximum_size_malformed_request(tmp_path: Path) -> None:
    prefix = b'{"version":1,"request_id":"'
    suffix = b'","command":"status","arguments":[]}'
    request_id = b'x' * (_MAX_MESSAGE_BYTES - len(prefix) - len(suffix))
    body = prefix + request_id + suffix
    assert len(body) == _MAX_MESSAGE_BYTES

    errors: list[AssertionError | OSError | ValueError] = []
    responses: list[dict[str, object]] = []
    with ControlServer(tmp_path / 'state', lambda _request, _identity: {}) as server:

        def send_request() -> None:
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(2)
                    client.connect(os.fspath(server._path))
                    client.sendall(struct.pack('!I', len(body)) + body)
                    responses.append(_read_response(client))
            except (AssertionError, OSError, ValueError) as error:
                errors.append(error)

        client = threading.Thread(target=send_request)
        client.start()
        assert server.serve_once(1.0)
        client.join(3)

    assert not client.is_alive()
    assert errors == []
    [response] = responses
    assert response['request_id'] == ''
    assert response['ok'] is False
    assert response['result'] is None
    assert isinstance(response['error'], str)
    assert response['error']


def test_server_restores_umask_when_bind_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    real_socket = socket.socket
    observed_umasks: list[int] = []

    def _current_umask() -> int:
        value = os.umask(0)
        os.umask(value)
        return value

    class FailingSocket(real_socket):
        def bind(self, _address: object) -> None:
            observed_umasks.append(_current_umask())
            raise OSError('simulated bind failure')

    monkeypatch.setattr(control_module.socket, 'socket', FailingSocket)
    original_umask = os.umask(0)
    try:
        server = ControlServer(tmp_path / 'state', lambda _request, _identity: {})
        with pytest.raises(OSError, match='simulated bind failure'):
            server._open()
        restored_umask = _current_umask()
    finally:
        os.umask(original_umask)

    assert observed_umasks == [0o177]
    assert restored_umask == 0


def test_server_serve_once_advances_foreground_progress_at_request_boundaries(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / 'state'
    events: list[str] = []

    def handler(
        _request: ControlRequest,
        _identity: PeerIdentity,
    ) -> Mapping[str, object]:
        events.append('handler')
        return {'accepted': True}

    with ControlServer(state_dir, handler) as server:
        server.set_foreground_progress(lambda: events.append('progress'))
        client = threading.Thread(
            target=lambda: ControlClient.for_state_dir(state_dir).request('status')
        )
        client.start()
        server.serve_once(1.0)
        client.join(2.0)

    assert events == ['progress', 'handler', 'progress', 'progress']


@pytest.mark.parametrize(
    'payload',
    [
        pytest.param(b'', id='silent'),
        pytest.param(struct.pack('!I', 8) + b'{', id='partial-frame'),
    ],
)
def test_server_serve_once_bounds_stalled_peer(
    monkeypatch: pytest.MonkeyPatch,
    payload: bytes,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(control_module, 'REQUEST_TIMEOUT_SECS', 0.05)
    with (
        ControlServer(tmp_path / 'state', lambda _request, _identity: {}) as server,
        socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client,
    ):
        client.settimeout(1)
        client.connect(os.fspath(server._path))
        client.sendall(payload)
        started_at = time.monotonic()

        assert server.serve_once(0.0)

        elapsed = time.monotonic() - started_at
        assert client.recv(1) == b''

    assert 0.03 <= elapsed < 1.0


def test_server_serve_once_dispatches_on_calling_thread(tmp_path: Path) -> None:
    state_dir = tmp_path / 'state'
    caller = threading.current_thread()
    handled_by: list[threading.Thread] = []

    def handler(
        _request: ControlRequest,
        _identity: PeerIdentity,
    ) -> Mapping[str, object]:
        handled_by.append(threading.current_thread())
        return {'accepted': True}

    with ControlServer(state_dir, handler) as server:
        result: list[Mapping[str, object]] = []
        client = threading.Thread(
            target=lambda: result.append(ControlClient.for_state_dir(state_dir).request('status'))
        )
        client.start()
        server.serve_once(1.0)
        client.join(2)

    assert result == [{'accepted': True}]
    assert handled_by == [caller]


def test_server_serve_once_zero_is_nonblocking_without_a_peer(tmp_path: Path) -> None:
    with ControlServer(tmp_path / 'state', lambda _request, _identity: {}) as server:
        started_at = time.monotonic()

        served = server.serve_once(0.0)

        elapsed = time.monotonic() - started_at

    assert not served
    assert elapsed < 1.0


def test_server_serve_once_zero_services_a_ready_peer(tmp_path: Path) -> None:
    body = b'{"version":1,"request_id":"ready","command":"status","arguments":{}}'
    with (
        ControlServer(
            tmp_path / 'state',
            lambda _request, _identity: {'accepted': True},
        ) as server,
        socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client,
    ):
        client.settimeout(1)
        client.connect(os.fspath(server._path))
        client.sendall(struct.pack('!I', len(body)) + body)

        assert server.serve_once(0.0)
        response = _read_response(client)

    assert response['ok'] is True
    assert response['result'] == {'accepted': True}


def test_server_unbinds_foreground_progress(tmp_path: Path) -> None:
    state_dir = tmp_path / 'state'
    advances: list[None] = []
    result: list[Mapping[str, object]] = []
    with ControlServer(state_dir, lambda _request, _identity: {'accepted': True}) as server:
        server.set_foreground_progress(lambda: advances.append(None))
        server.set_foreground_progress(None)
        client = threading.Thread(
            target=lambda: result.append(ControlClient.for_state_dir(state_dir).request('status'))
        )
        client.start()
        try:
            assert server.serve_once(1.0)
        finally:
            server._close()
            client.join(2.0)

    assert not client.is_alive()
    assert result == [{'accepted': True}]
    assert advances == []


def test_server_uses_platform_peer_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    identity = PeerIdentity(uid=os.geteuid(), gid=os.getegid(), pid=101)
    observed: list[PeerIdentity] = []

    def selected_identity(_socket: socket.socket) -> PeerIdentity:
        return identity

    monkeypatch.setattr(control_module, 'peer_identity', selected_identity)

    def handler(
        _request: ControlRequest,
        peer: PeerIdentity,
    ) -> Mapping[str, object]:
        observed.append(peer)
        return {'ok': True}

    stop = threading.Event()
    with ControlServer(tmp_path / 'state', handler) as server:
        thread = threading.Thread(target=_run_server, args=(server, stop))
        try:
            thread.start()
            result = ControlClient(server._path).request('status')
        finally:
            stop.set()
            if thread.ident is not None:
                thread.join(2)
                assert not thread.is_alive()

    assert result == {'ok': True}
    assert observed == [identity]


def test_status_budget_shared_json_body_encoder_preserves_control_encoding() -> None:
    assert encode_json_body({'text': '£', 'value': 1.5}) == (b'{"text":"\\u00a3","value":1.5}')
    encode_frame = cast(
        'Callable[[Mapping[str, object]], bytes]',
        vars(control_module)['_encode_frame'],
    )
    body = b'{"text":"\\u00a3","value":1.5}'
    assert encode_frame({'text': '£', 'value': 1.5}) == struct.pack('!I', len(body)) + body
    oversized_value = {'text': 'x' * _MAX_MESSAGE_BYTES}
    assert encode_json_body(oversized_value) == b'{"text":"' + b'x' * _MAX_MESSAGE_BYTES + b'"}'
    with pytest.raises(
        ControlProtocolError,
        match=f'^control message exceeds maximum of {_MAX_MESSAGE_BYTES} bytes$',
    ):
        encode_frame(oversized_value)

    for invalid_value, cause in (
        (float('nan'), ValueError),
        (float('inf'), ValueError),
        (float('-inf'), ValueError),
        (object(), TypeError),
    ):
        with pytest.raises(
            ControlProtocolError,
            match=r'^control message is not JSON-compatible$',
        ) as caught:
            encode_json_body({'value': invalid_value})
        assert isinstance(caught.value.__cause__, cause)
