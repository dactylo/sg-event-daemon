from __future__ import annotations

import socket
import threading
import time
from contextlib import contextmanager
from dataclasses import FrozenInstanceError
from queue import Queue
from typing import TYPE_CHECKING, cast

import pytest

import sg_event_daemon._probe as probe_module
from sg_event_daemon._probe import ProbeResult, ProbeSnapshot, SupervisorProgress

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from typing import Self


_BAD_REQUEST_RESPONSE = (
    b'HTTP/1.1 400 Bad Request\r\n'
    b'Content-Type: text/plain; charset=us-ascii\r\n'
    b'Content-Length: 12\r\n'
    b'Connection: close\r\n'
    b'\r\n'
    b'bad request\n'
)
_METHOD_NOT_ALLOWED_RESPONSE = (
    b'HTTP/1.1 405 Method Not Allowed\r\n'
    b'Content-Type: text/plain; charset=us-ascii\r\n'
    b'Content-Length: 19\r\n'
    b'Connection: close\r\n'
    b'\r\n'
    b'method not allowed\n'
)
_NOT_FOUND_RESPONSE = (
    b'HTTP/1.1 404 Not Found\r\n'
    b'Content-Type: text/plain; charset=us-ascii\r\n'
    b'Content-Length: 10\r\n'
    b'Connection: close\r\n'
    b'\r\n'
    b'not found\n'
)
_OK_RESPONSE = (
    b'HTTP/1.1 200 OK\r\n'
    b'Content-Type: text/plain; charset=us-ascii\r\n'
    b'Content-Length: 3\r\n'
    b'Connection: close\r\n'
    b'\r\n'
    b'ok\n'
)
_UNAVAILABLE_RESPONSE = (
    b'HTTP/1.1 503 Service Unavailable\r\n'
    b'Content-Type: text/plain; charset=us-ascii\r\n'
    b'Content-Length: 12\r\n'
    b'Connection: close\r\n'
    b'\r\n'
    b'unavailable\n'
)


def _exchange(
    server: probe_module.ProbeServer,
    request: bytes,
    *,
    family: int = socket.AF_INET,
) -> bytes:
    listener = server._listener
    assert listener is not None
    address = listener.getsockname()
    with socket.socket(family, socket.SOCK_STREAM) as client:
        client.settimeout(2.0)
        client.connect(address)
        client.sendall(request)
        chunks: list[bytes] = []
        while chunk := client.recv(4_096):
            chunks.append(chunk)
    return b''.join(chunks)


def _healthy_snapshot(*, generated_at: float = 10.0) -> ProbeSnapshot:
    return ProbeSnapshot(
        generated_at=generated_at,
        ready=True,
        complete=True,
        conditions=('healthy',),
        shutting_down=False,
        halted=False,
    )


def _result(*, liveness: bool = True, readiness: bool = True) -> ProbeResult:
    return ProbeResult(
        liveness=liveness,
        readiness=readiness,
        complete=True,
        conditions=('secret-diagnostic',),
        generated_at=1.0,
        snapshot_age_seconds=0.0,
    )


@contextmanager
def _running_probe(
    result: Callable[[], ProbeResult],
    *,
    bind: str = '127.0.0.1',
    peer_timeout_secs: float = 1.0,
) -> Generator[probe_module.ProbeServer]:
    server = probe_module.ProbeServer(
        bind,
        0,
        result,
        peer_timeout_secs=peer_timeout_secs,
    )
    server.start()
    try:
        yield server
    finally:
        server.close()
        assert server.join(timeout=2.0)


@pytest.fixture
def peer_starts(monkeypatch: pytest.MonkeyPatch) -> Queue[socket.socket]:
    started: Queue[socket.socket] = Queue()
    serve_peer = probe_module.ProbeServer._serve_peer

    def observe_peer(server: probe_module.ProbeServer, peer: socket.socket) -> None:
        started.put(peer)
        serve_peer(server, peer)

    monkeypatch.setattr(probe_module.ProbeServer, '_serve_peer', observe_peer)
    return started


def test_close_interrupts_peer_before_its_serving_thread_closes_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started: Queue[socket.socket] = Queue()
    release = threading.Event()
    serve_peer = probe_module.ProbeServer._serve_peer

    def serve_after_close(server: probe_module.ProbeServer, peer: socket.socket) -> None:
        started.put(peer)
        assert release.wait(2.0)
        serve_peer(server, peer)

    monkeypatch.setattr(probe_module.ProbeServer, '_serve_peer', serve_after_close)
    with _running_probe(_result, peer_timeout_secs=10.0) as server:
        with socket.create_connection(server._listener.getsockname(), timeout=1.0):
            try:
                peer = started.get(timeout=1.0)
                peer.settimeout(1.0)
                server.close()
                # Shutdown cancels I/O without invalidating the serving owner's descriptor.
                assert peer.recv(1) == b''
            finally:
                release.set()
            assert server.join(timeout=2.0)
            assert peer.fileno() == -1
        assert server.failure_reason is None


def test_close_join_waits_for_peer_thread_after_service_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleaned_up = threading.Event()
    release = threading.Event()
    serve_admitted = probe_module.ProbeServer._serve_admitted

    def finish_later(server: probe_module.ProbeServer, peer: socket.socket) -> None:
        serve_admitted(server, peer)
        cleaned_up.set()
        assert release.wait(2.0)

    monkeypatch.setattr(probe_module.ProbeServer, '_serve_admitted', finish_later)
    with _running_probe(_result) as server:
        try:
            assert _exchange(server, b'GET /livez HTTP/1.1\r\n\r\n') == _OK_RESPONSE
            assert cleaned_up.wait(1.0)
            server.close()
            assert not server.join(timeout=0.2)
        finally:
            release.set()
        assert server.join(timeout=2.0)


def test_close_joins_accept_and_peer_threads(peer_starts: Queue[socket.socket]) -> None:
    with _running_probe(_result, peer_timeout_secs=10.0) as server:
        with socket.create_connection(server._listener.getsockname(), timeout=1.0):
            peer_starts.get(timeout=1.0)
            server.close()
            assert server.join(timeout=2.0)
        assert server.failure_reason is None


def test_idle_peer_does_not_block_another_admitted_peer(
    peer_starts: Queue[socket.socket],
) -> None:
    with (
        _running_probe(_result) as server,
        socket.create_connection(server._listener.getsockname(), timeout=1.0),
    ):
        peer_starts.get(timeout=1.0)
        started_at = time.monotonic()
        response = _exchange(server, b'GET /livez HTTP/1.1\r\n\r\n')
        elapsed = time.monotonic() - started_at

    assert response == _OK_RESPONSE
    assert elapsed < 0.5


def test_ipv6_readyz_uses_ipv6_listener_when_loopback_is_available() -> None:
    if not socket.has_ipv6:
        pytest.skip('IPv6 is unavailable')
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as listener:
            listener.bind(('::1', 0))
    except OSError as error:
        pytest.skip(f'IPv6 loopback is unavailable: {error}')

    with _running_probe(_result, bind='::1') as server:
        response = _exchange(
            server,
            b'GET /readyz HTTP/1.1\r\n\r\n',
            family=socket.AF_INET6,
        )

    assert response == _OK_RESPONSE


@pytest.mark.parametrize(
    ('raw_request', 'result', 'expected'),
    [
        pytest.param(b'GET /livez HTTP/1.1\r\n\r\n', _result(), _OK_RESPONSE, id='get-livez'),
        pytest.param(
            b'GET /readyz HTTP/1.1\r\n\r\n',
            _result(readiness=False),
            _UNAVAILABLE_RESPONSE,
            id='get-readyz',
        ),
        pytest.param(
            b'HEAD /livez HTTP/1.1\r\n\r\n',
            _result(),
            _OK_RESPONSE.removesuffix(b'ok\n'),
            id='head-livez',
        ),
        pytest.param(
            b'HEAD /readyz HTTP/1.1\r\n\r\n',
            _result(readiness=False),
            _UNAVAILABLE_RESPONSE.removesuffix(b'unavailable\n'),
            id='head-readyz',
        ),
    ],
)
def test_livez_and_readyz_return_fixed_body_free_status(
    raw_request: bytes,
    result: ProbeResult,
    expected: bytes,
) -> None:
    calls: list[str] = []

    with _running_probe(lambda: calls.append('result') or result) as server:
        response = _exchange(server, raw_request)

    assert response == expected
    assert calls == ['result']
    assert b'Server:' not in response
    assert b'Date:' not in response
    assert b'secret-diagnostic' not in response


@pytest.mark.parametrize(
    ('raw_request', 'expected'),
    [
        pytest.param(b'GET /missing HTTP/1.1\r\n\r\n', _NOT_FOUND_RESPONSE, id='path'),
        pytest.param(b'GET /livez?full=1 HTTP/1.1\r\n\r\n', _NOT_FOUND_RESPONSE, id='query'),
        pytest.param(
            b'HEAD /missing HTTP/1.1\r\n\r\n',
            _NOT_FOUND_RESPONSE.removesuffix(b'not found\n'),
            id='head-path',
        ),
        pytest.param(
            b'HEAD /readyz?full=1 HTTP/1.1\r\n\r\n',
            _NOT_FOUND_RESPONSE.removesuffix(b'not found\n'),
            id='head-query',
        ),
        pytest.param(
            b'POST /livez HTTP/1.1\r\n\r\n',
            _METHOD_NOT_ALLOWED_RESPONSE,
            id='method',
        ),
        pytest.param(b'not-http\r\n\r\n', _BAD_REQUEST_RESPONSE, id='malformed'),
    ],
)
def test_livez_and_readyz_reject_nonexact_requests_without_result_lookup(
    raw_request: bytes,
    expected: bytes,
) -> None:
    calls: list[str] = []

    with _running_probe(lambda: calls.append('result') or _result()) as server:
        response = _exchange(server, raw_request)

    assert response == expected
    assert calls == []


def test_livez_connection_serves_only_one_request() -> None:
    with _running_probe(_result) as server:
        response = _exchange(
            server,
            b'GET /livez HTTP/1.1\r\n\r\nGET /readyz HTTP/1.1\r\n\r\n',
        )

    assert response == _OK_RESPONSE
    assert response.count(b'HTTP/1.1') == 1


def test_livez_listener_records_unexpected_exit_as_fatal_reason() -> None:
    server = probe_module.ProbeServer('127.0.0.1', 0, _result)
    server.start()
    listener = server._listener
    assert listener is not None
    listener.close()

    assert server.join(timeout=1.0)
    assert server.failure_reason == 'probe listener exited unexpectedly'
    server.close()


@pytest.mark.parametrize(
    'raw_request',
    [
        pytest.param(b'GET /' + (b'x' * 1_024) + b' HTTP/1.1\r\n\r\n', id='request-line'),
        pytest.param(
            b'GET /livez HTTP/1.1\r\nX-Fill: ' + (b'x' * 8_192) + b'\r\n\r\n',
            id='headers',
        ),
    ],
)
def test_livez_rejects_oversize_request_sections(raw_request: bytes) -> None:
    with _running_probe(_result) as server:
        response = _exchange(server, raw_request)

    assert response == _BAD_REQUEST_RESPONSE


def test_parseable_head_timed_out_headers_select_body_free_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses: list[bytes] = []

    class _EarlyTimeoutPeer:
        def __init__(self) -> None:
            self._reads = 0

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_details: object) -> None:
            return None

        def recv(self, _maximum: int) -> bytes:
            self._reads += 1
            if self._reads == 1:
                return b'HEAD /livez HTTP/1.1\r\nX-Wait: '
            raise TimeoutError

        def sendall(self, response: bytes) -> None:
            responses.append(response)

        def settimeout(self, _timeout: float) -> None:
            return None

    monkeypatch.setattr(probe_module, 'monotonic', lambda: 0.0)
    server = probe_module.ProbeServer('127.0.0.1', 0, _result)
    try:
        serve_peer = probe_module.ProbeServer._serve_peer
        serve_peer(server, cast('socket.socket', _EarlyTimeoutPeer()))
    finally:
        server.close()

    assert responses == [_BAD_REQUEST_RESPONSE.removesuffix(b'bad request\n')]


def test_peer_disconnect_is_not_fatal(peer_starts: Queue[socket.socket]) -> None:
    with _running_probe(_result) as server:
        with socket.create_connection(server._listener.getsockname(), timeout=1.0):
            peer_starts.get(timeout=1.0)
        assert _exchange(server, b'GET /livez HTTP/1.1\r\n\r\n') == _OK_RESPONSE
    assert server.failure_reason is None


def test_probe_closes_new_peer_while_all_slots_are_taken(
    monkeypatch: pytest.MonkeyPatch,
    peer_starts: Queue[socket.socket],
) -> None:
    monkeypatch.setattr(probe_module, '_MAX_PROBE_PEERS', 2, raising=False)
    with _running_probe(_result) as server:
        address = server._listener.getsockname()
        with (
            socket.create_connection(address, timeout=1.0),
            socket.create_connection(address, timeout=1.0),
        ):
            peer_starts.get(timeout=0.5)
            peer_starts.get(timeout=0.5)
            with socket.create_connection(address, timeout=0.5) as excess:
                assert excess.recv(1) == b''


def test_probe_serializes_result_callbacks_and_joins_waiting_peers(
    peer_starts: Queue[socket.socket],
) -> None:
    first_entered = threading.Event()
    second_entered = threading.Event()
    release = threading.Event()

    def delayed_result() -> ProbeResult:
        if first_entered.is_set():
            second_entered.set()
        else:
            first_entered.set()
            assert release.wait(2.0)
        return _result()

    with _running_probe(delayed_result) as server:
        address = server._listener.getsockname()
        with (
            socket.create_connection(address, timeout=1.0) as first,
            socket.create_connection(address, timeout=1.0) as second,
        ):
            try:
                first.sendall(b'GET /livez HTTP/1.1\r\n\r\n')
                assert first_entered.wait(1.0)
                second.sendall(b'GET /livez HTTP/1.1\r\n\r\n')
                peer_starts.get(timeout=0.5)
                peer_starts.get(timeout=0.5)
                assert not second_entered.wait(0.05)
                server.close()
                started_at = time.monotonic()
                assert not server.join(timeout=0.05)
                assert time.monotonic() - started_at < 0.5
            finally:
                release.set()
            assert server.join(timeout=2.0)
            assert second_entered.is_set()


def test_probe_server_start_failure_leaves_join_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server = probe_module.ProbeServer('127.0.0.1', 0, _result)

    class FailedThread:
        def __init__(self, **_options: object) -> None:
            return None

        def is_alive(self) -> bool:
            return False

        def join(self, _timeout: float) -> None:
            pytest.fail('unstarted probe thread was joined')

        def start(self) -> None:
            raise RuntimeError('probe thread start failed')

    monkeypatch.setattr(probe_module.threading, 'Thread', FailedThread)

    with pytest.raises(RuntimeError, match='probe thread start failed'):
        server.start()

    server.close()
    assert server.join(timeout=0.0)
    assert server._thread is None


def test_probe_snapshot_is_immutable() -> None:
    snapshot = _healthy_snapshot()

    with pytest.raises(FrozenInstanceError):
        snapshot.ready = False  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.parametrize(
    ('snapshot', 'expected_conditions'),
    [
        pytest.param(None, (), id='no-snapshot'),
        pytest.param(
            ProbeSnapshot(
                generated_at=5.0,
                ready=True,
                complete=False,
                conditions=('healthy',),
                shutting_down=False,
                halted=False,
            ),
            ('healthy',),
            id='incomplete',
        ),
        pytest.param(
            ProbeSnapshot(
                generated_at=5.0,
                ready=True,
                complete=True,
                conditions=('healthy',),
                shutting_down=True,
                halted=False,
            ),
            ('healthy',),
            id='shutdown',
        ),
        pytest.param(
            ProbeSnapshot(
                generated_at=5.0,
                ready=True,
                complete=True,
                conditions=('healthy',),
                shutting_down=False,
                halted=True,
            ),
            ('healthy',),
            id='halted',
        ),
    ],
)
def test_probe_states_fail_closed(
    snapshot: ProbeSnapshot | None,
    expected_conditions: tuple[str, ...],
) -> None:
    progress = SupervisorProgress(
        supervisor_stale_after=10.0,
        readiness_snapshot_stale_after=10.0,
    )
    progress.advance(now=5.0)
    if snapshot is not None:
        progress.publish(snapshot)

    result = progress.result(now=6.0)

    assert result.conditions == expected_conditions
    assert not result.readiness
    assert result.liveness is (snapshot is None or not snapshot.halted)


@pytest.mark.parametrize(
    'raw_request',
    [
        pytest.param(
            b'HEAD /livez HTTP/1.1\r\nMissing-Colon\r\n\r\n',
            id='malformed-header',
        ),
        pytest.param(
            b'HEAD /readyz HTTP/1.1\r\nX-Fill: ' + (b'x' * 8_192) + b'\r\n\r\n',
            id='oversize-header',
        ),
    ],
)
def test_parseable_head_bad_headers_return_body_free_400(raw_request: bytes) -> None:
    with _running_probe(_result) as server:
        response = _exchange(server, raw_request)

    assert response == _BAD_REQUEST_RESPONSE.removesuffix(b'bad request\n')


def test_probe_write_uses_only_budget_remaining_after_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 0.0
    timeouts: list[float] = []

    class _NearDeadlinePeer:
        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_details: object) -> None:
            return None

        def recv(self, _maximum: int) -> bytes:
            nonlocal now
            now = 0.09
            return b'GET /livez HTTP/1.1\r\n\r\n'

        def sendall(self, _response: bytes) -> None:
            raise TimeoutError

        def settimeout(self, timeout: float) -> None:
            timeouts.append(timeout)

    monkeypatch.setattr(probe_module, 'monotonic', lambda: now, raising=False)
    server = probe_module.ProbeServer('127.0.0.1', 0, _result, peer_timeout_secs=0.1)
    try:
        serve_peer = probe_module.ProbeServer._serve_peer
        serve_peer(server, cast('socket.socket', _NearDeadlinePeer()))
    finally:
        server.close()

    assert timeouts == pytest.approx([0.1, 0.01])


def test_progress_fails_liveness_at_exact_stale_boundary() -> None:
    progress = SupervisorProgress(
        supervisor_stale_after=10.0,
        readiness_snapshot_stale_after=4.0,
    )
    progress.advance(now=20.0)
    progress.publish(_healthy_snapshot(generated_at=29.0))

    assert progress.result(now=29.999).liveness
    assert not progress.result(now=30.0).liveness


def test_reader_thread_cannot_refresh_frozen_foreground_heartbeat() -> None:
    progress = SupervisorProgress(
        supervisor_stale_after=2.0,
        readiness_snapshot_stale_after=10.0,
    )
    progress.advance(now=5.0)
    progress.publish(_healthy_snapshot(generated_at=5.0))
    results: list[bool] = []

    reader = threading.Thread(target=lambda: results.append(progress.result(now=7.0).liveness))
    reader.start()
    reader.join(1.0)

    assert results == [False]


def test_slow_client_releases_its_probe_thread_within_peer_budget() -> None:
    with _running_probe(_result, peer_timeout_secs=0.12) as server:
        threads_before = set(threading.enumerate())
        listener = server._listener
        assert listener is not None
        address = listener.getsockname()
        with socket.create_connection(address, timeout=1.0) as slow_client:
            slow_client.sendall(b'GET /livez HTTP/1.1\r\nX-Wait: ')
            stop_trickling = threading.Event()
            first_trickle = threading.Event()

            def trickle_past_watchdog() -> None:
                for _index in range(50):
                    if stop_trickling.wait(0.03):
                        return
                    try:
                        slow_client.sendall(b'x')
                    except OSError:
                        return
                    first_trickle.set()

            trickler = threading.Thread(target=trickle_past_watchdog)
            trickler.start()
            try:
                assert first_trickle.wait(1.0)
                started_at = time.monotonic()
                response = _exchange(server, b'GET /readyz HTTP/1.1\r\n\r\n')
                elapsed = time.monotonic() - started_at
            finally:
                stop_trickling.set()
                trickler.join(2.0)
            assert not trickler.is_alive()
            assert slow_client.recv(1) == b''
        for thread in set(threading.enumerate()) - threads_before:
            thread.join(1.0)
        threads_after = set(threading.enumerate())

    assert response == _OK_RESPONSE
    assert elapsed < 1.0
    assert threads_after == threads_before


def test_snapshot_fails_readiness_at_exact_freshness_boundary() -> None:
    progress = SupervisorProgress(
        supervisor_stale_after=10.0,
        readiness_snapshot_stale_after=4.0,
    )
    progress.advance(now=13.0)
    progress.publish(_healthy_snapshot(generated_at=10.0))

    fresh = progress.result(now=13.999)
    stale = progress.result(now=14.0)

    assert fresh.liveness
    assert fresh.readiness
    assert stale.liveness
    assert not stale.readiness
    assert stale.snapshot_age_seconds == 4.0


def test_snapshot_rejects_mutable_conditions() -> None:
    with pytest.raises(TypeError, match='tuple'):
        ProbeSnapshot(
            generated_at=1.0,
            ready=True,
            complete=True,
            conditions=['healthy'],  # pyright: ignore[reportArgumentType]
            shutting_down=False,
            halted=False,
        )


def test_successful_empty_snapshot_is_ready() -> None:
    progress = SupervisorProgress(
        supervisor_stale_after=10.0,
        readiness_snapshot_stale_after=10.0,
    )
    progress.advance(now=5.0)
    progress.publish(_healthy_snapshot(generated_at=5.0))

    result = progress.result(now=5.0)

    assert result.liveness
    assert result.readiness
    assert result.complete
    assert result.generated_at == 5.0
    assert result.snapshot_age_seconds == 0.0


def test_unexpected_peer_exception_is_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    errors: list[BaseException] = []
    failed = threading.Event()

    def record_failure(args: threading.ExceptHookArgs) -> None:
        assert args.exc_value is not None
        errors.append(args.exc_value)
        failed.set()

    def broken_result() -> ProbeResult:
        raise RuntimeError('probe result failed')

    monkeypatch.setattr(threading, 'excepthook', record_failure)
    with _running_probe(broken_result) as server:
        address = server._listener.getsockname()
        assert _exchange(server, b'GET /livez HTTP/1.1\r\n\r\n') == b''
        assert failed.wait(1.0)
        assert server.join(timeout=1.0)
        assert server.failure_reason == 'probe listener exited unexpectedly'
        with pytest.raises(ConnectionRefusedError), socket.create_connection(address, timeout=0.5):
            pytest.fail('failed probe listener still accepted a connection')

    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)


@pytest.mark.parametrize('condition', ['source-degraded', 'stale', 'worker-breaker-open'])
def test_worker_repair_state_affects_readiness_not_liveness(condition: str) -> None:
    progress = SupervisorProgress(
        supervisor_stale_after=10.0,
        readiness_snapshot_stale_after=10.0,
    )
    progress.advance(now=5.0)
    progress.publish(
        ProbeSnapshot(
            generated_at=5.0,
            ready=False,
            complete=True,
            conditions=(condition,),
            shutting_down=False,
            halted=False,
        )
    )

    result = progress.result(now=6.0)

    assert result.liveness
    assert not result.readiness
    assert result.conditions == (condition,)


@pytest.mark.parametrize('threshold', [float('nan'), float('inf'), 0.0, -1.0])
def test_progress_rejects_invalid_stale_thresholds(threshold: float) -> None:
    with pytest.raises(ValueError, match='threshold'):
        SupervisorProgress(
            supervisor_stale_after=threshold,
            readiness_snapshot_stale_after=1.0,
        )

    with pytest.raises(ValueError, match='threshold'):
        SupervisorProgress(
            supervisor_stale_after=1.0,
            readiness_snapshot_stale_after=threshold,
        )


@pytest.mark.parametrize('now', [float('nan'), float('inf'), -1.0])
def test_progress_rejects_invalid_observation_times(now: float) -> None:
    progress = SupervisorProgress(
        supervisor_stale_after=1.0,
        readiness_snapshot_stale_after=1.0,
    )

    with pytest.raises(ValueError, match='time'):
        progress.advance(now=now)
    with pytest.raises(ValueError, match='time'):
        progress.result(now=now)


@pytest.mark.parametrize('generated_at', [float('nan'), float('inf'), -1.0])
def test_snapshot_rejects_invalid_generation_time(generated_at: float) -> None:
    with pytest.raises(ValueError, match='time'):
        _healthy_snapshot(generated_at=generated_at)
