from __future__ import annotations

import errno
import json
import os
import select
import selectors
import socket
import stat
import struct
import tempfile
import threading
import time
from collections import deque
from contextlib import closing, contextmanager
from dataclasses import MISSING, fields, replace
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import TYPE_CHECKING, Literal, cast
from unittest.mock import Mock

import pytest

import sg_event_daemon._fanout as fanout_module
import sg_event_daemon._ipc as ipc_module
from sg_event_daemon._events import Event, decode_event, encode_event
from sg_event_daemon._fanout import (
    EncodedEvent,
    EventDelivery,
    EventFanout,
    GroupChannel,
    GroupChannelInterface,
    StrictCapacityControl,
    StrictOrderingControl,
)
from sg_event_daemon._group_types import (
    AuditIdentity,
    ConditionalParkDirective,
    LiveJoinIdentity,
)
from sg_event_daemon._health import (
    EventSocketHealth,
    GroupHealth,
    WatchdogHealth,
    WorkerHealth,
    aggregate_status,
)
from sg_event_daemon._ipc import (
    _CONTROL_PROTOCOL_VERSION,
    _EVENT_AUTH_PROTOCOL_VERSION,
    ActiveSourceGroupAssignment,
    CommandResult,
    ControlHello,
    ControlMessage,
    EventClient,
    EventServer,
    GroupBootstrapObservation,
    GroupBootstrapPending,
    GroupBootstrapRequest,
    HealthUpdate,
    OperatorCommand,
    PendingDispositionSourceGroupAssignment,
    StopCommand,
    WorkerAssignment,
    WorkerControl,
    _decode_control_body,
    _decode_event_wire,
    _encode_fanout_message,
    _FrameError,
    _FrameReader,
    _IPCAuthenticationError,
    _OutboundFrame,
    _PartialWriter,
    _unpack_length_frame,
    encode_control_message,
)
from sg_event_daemon._platform import SecurityError
from sg_event_daemon._source import (
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
    SourceRunStatus,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterator, Mapping

_ATTEMPT_ID = 'attempt-1'
_MAX_FRAME_BYTES = 131_072
_QUEUE_BYTES = 262_144
_SHORT_SOCKET_ROOT: list[Path] = []


class _AlwaysBlockingSocket:
    def __init__(self, sock: socket.socket) -> None:
        self._socket = sock

    def close(self) -> None:
        self._socket.close()

    def fileno(self) -> int:
        return self._socket.fileno()

    @staticmethod
    def recv(_size: int) -> bytes:
        raise BlockingIOError

    @staticmethod
    def send(_value: memoryview) -> int:
        raise BlockingIOError


class _JoinSourceStub:
    def abort(self, abort: JoinAbort) -> None:
        raise AssertionError(abort)

    def acknowledge_strict_control(
        self,
        control: StrictCapacityControl | StrictOrderingControl,
    ) -> None:
        raise AssertionError(control)

    def confirm(self, confirmation: JoinConfirm) -> Attached:
        raise AssertionError(confirmation)

    def event_channel_lost(self, group: str, *, reason: str) -> None:
        raise AssertionError((group, reason))

    def finalize(self, finalization: JoinFinalize) -> JoinFinalized:
        raise AssertionError(finalization)

    def observe_group(
        self,
        request: GroupBootstrapRequest,
    ) -> GroupBootstrapObservation:
        raise AssertionError(request)

    def request(
        self,
        request: JoinRequest,
        channel: GroupChannelInterface,
    ) -> JoinTarget:
        raise AssertionError((request, channel))


class _ScriptedRecvSocket:
    def __init__(
        self,
        sock: socket.socket,
        outcomes: tuple[bytes | OSError, ...],
    ) -> None:
        self._outcomes = deque(outcomes)
        self._socket = sock

    def close(self) -> None:
        self._socket.close()

    def fileno(self) -> int:
        return self._socket.fileno()

    def getsockopt(
        self,
        level: int,
        option: int,
        length: int = 0,
    ) -> int | bytes:
        return self._socket.getsockopt(level, option, length)

    def recv(self, _size: int) -> bytes:
        outcome = self._outcomes.popleft()
        if isinstance(outcome, OSError):
            raise outcome
        returned = outcome[:_size]
        remaining = outcome[_size:]
        if remaining:
            self._outcomes.appendleft(remaining)
        return returned


class _ScriptedStartupSocket:
    def __init__(
        self,
        sock: socket.socket,
        *,
        connect_action: Callable[[float | None], None] | None = None,
        send_action: Callable[[memoryview, float | None], int] | None = None,
    ) -> None:
        self._connect_action = connect_action
        self._send_action = send_action
        self._socket = sock
        self._timeout: float | None = None

    def close(self) -> None:
        self._socket.close()

    def connect(self, _path: str) -> None:
        if self._connect_action is not None:
            self._connect_action(self._timeout)

    def fileno(self) -> int:
        return self._socket.fileno()

    def getsockopt(self, level: int, option: int) -> int:
        return int(self._socket.getsockopt(level, option))

    def send(self, value: memoryview) -> int:
        if self._send_action is None:
            return len(value)
        return self._send_action(value, self._timeout)

    def setsockopt(self, level: int, option: int, value: int) -> None:
        self._socket.setsockopt(level, option, value)

    def settimeout(self, timeout: float | None) -> None:
        self._timeout = timeout
        self._socket.settimeout(timeout)


class _SourceThreadHarness:
    def __init__(self, server: EventServer, source: _JoinSourceStub) -> None:
        self._action_complete = threading.Event()
        self._actions: deque[Callable[[], None]] = deque()
        self._actions_lock = threading.Lock()
        self._errors: deque[Exception] = deque()
        self._owner: int | None = None
        self._server = server
        self._source = source
        self._started = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run)

    def _call(self, action: Callable[[], None]) -> None:
        self._action_complete.clear()
        with self._actions_lock:
            self._actions.append(action)
        if not self._action_complete.wait(1.0):
            raise AssertionError('source-thread action did not complete')
        if self._errors:
            raise self._errors.popleft()

    def _close(self) -> None:
        self._stop.set()
        if self._thread.ident is None:
            return
        self._thread.join(1.0)
        if self._thread.is_alive():
            raise AssertionError('source-owner thread did not stop')

    def _owner_ident(self) -> int | None:
        return self._owner

    def _run(self) -> None:
        self._owner = threading.get_ident()
        self._started.set()
        while not self._stop.is_set():
            self._server.poll(0.001)
            self._server.dispatch_join_requests(self._source)
            with self._actions_lock:
                action = self._actions.popleft() if self._actions else None
            if action is None:
                continue
            try:
                action()
            except (AssertionError, ConnectionError, _FrameError, KeyError) as error:
                self._errors.append(error)
            finally:
                self._action_complete.set()

    def _start(self) -> None:
        self._thread.start()
        if not self._started.wait(1.0):
            raise AssertionError('source-owner thread did not start')


class _SpuriousRecvSocket:
    def __init__(self, sock: socket.socket) -> None:
        self._raised = False
        self._socket = sock

    def close(self) -> None:
        self._socket.close()

    def fileno(self) -> int:
        return self._socket.fileno()

    def recv(self, size: int) -> bytes:
        if not self._raised:
            self._raised = True
            raise BlockingIOError
        return self._socket.recv(size)


class _SpuriousShortSendSocket:
    def __init__(self, sock: socket.socket, *, chunk_bytes: int) -> None:
        self._chunk_bytes = chunk_bytes
        self._raised = False
        self._socket = sock

    def close(self) -> None:
        self._socket.close()

    def fileno(self) -> int:
        return self._socket.fileno()

    def send(self, value: memoryview) -> int:
        if not self._raised:
            self._raised = True
            raise BlockingIOError
        return self._socket.send(value[: self._chunk_bytes])


class _StalledConnectSocket:
    def __init__(self, sock: socket.socket) -> None:
        self._socket = sock
        self._timeout: float | None = None

    def close(self) -> None:
        self._socket.close()

    def connect(self, _path: str) -> None:
        delay = 0.25 if self._timeout is None else self._timeout
        time.sleep(delay)
        raise TimeoutError

    def fileno(self) -> int:
        return self._socket.fileno()

    def getsockopt(self, level: int, option: int) -> int:
        return int(self._socket.getsockopt(level, option))

    def setsockopt(self, level: int, option: int, value: int) -> None:
        self._socket.setsockopt(level, option, value)

    def settimeout(self, timeout: float | None) -> None:
        self._timeout = timeout
        self._socket.settimeout(timeout)


class _StartupSelector:
    def __init__(self, *, fail_registration: bool) -> None:
        self._closed = False
        self._registered = False
        self._registration_attempted = False
        self._fail_registration = fail_registration

    def close(self) -> None:
        self._closed = True

    def register(self, _fileobj: object, _events: int) -> None:
        self._registration_attempted = True
        if self._fail_registration:
            raise OSError(errno.EBADF, 'scripted selector registration failure')
        self._registered = True


def _clamp_coordination_send_buffer(client: EventClient, requested: int) -> int:
    coordination = client._coordination_socket
    assert coordination is not None
    coordination.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, requested)
    return int(coordination.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF))


def _decode_control_message(
    frame: bytes, *, max_frame_bytes: int = _MAX_FRAME_BYTES
) -> ControlMessage:
    return _decode_control_body(_unpack_length_frame(frame, max_frame_bytes=max_frame_bytes))


def _decode_enum_payload(decoder: str, payload: dict[str, object]) -> object:
    if decoder == '_decode_auth_body':
        auth_decoder = cast('Callable[[bytes], object]', vars(ipc_module)[decoder])
        return auth_decoder(json.dumps(payload).encode())
    if decoder == '_decode_join_control':
        identity = payload['identity']
        assert isinstance(identity, dict)
        typed_identity = cast('dict[str, object]', identity)
        control_decoder = cast(
            'Callable[[dict[str, object], LiveJoinIdentity], object]', vars(ipc_module)[decoder]
        )
        return control_decoder(payload, ipc_module._wire_identity(typed_identity))
    decode = cast('Callable[[dict[str, object]], object]', vars(ipc_module)[decoder])
    return decode(payload)


def _event(event_id: int, *, text: str = 'payload') -> Event:
    return Event(
        {
            'id': event_id,
            'type': 'EventLogEntry',
            'event_type': 'Shotgun_Test',
            'attribute_name': None,
            'meta': {
                'active': True,
                'blob': b'\x00\xff',
                'day': date(2026, 7, 27),
                'items': [None, 4, 1.5, text],
                'when': datetime(2026, 7, 27, 12, 30, 1, 9, UTC),
            },
            'entity': {'id': 3, 'type': 'Asset'},
            'user': None,
            'project': {'id': 2, 'type': 'Project'},
            'session_uuid': 'session',
            'created_at': datetime(2026, 7, 27, 12, 30, 1, 9, UTC),
        }
    )


def _event_authentication_frame(
    role: Literal['coordination', 'event'],
    *,
    capability: str = 'capability-1',
    connection_id: str = 'connection-1',
) -> bytes:
    body = json.dumps(
        {
            'capability': capability,
            'connection_id': connection_id,
            'group': 'group-a',
            'role': role,
            'schema': 'sg-event-daemon-event-auth',
            'source': 'events',
            'spawn_instance': 'spawn-1',
            'version': 1,
        },
        ensure_ascii=True,
        separators=(',', ':'),
        sort_keys=True,
    ).encode()
    return struct.pack('!I', len(body)) + body


def _event_client(path: Path, group: str = 'group-a') -> EventClient:
    return EventClient(
        path=path,
        source='events',
        group=group,
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=1.0,
        stop=threading.Event(),
    )


def _frame_bytes(frame: _OutboundFrame) -> bytes:
    return b''.join(frame._parts)


def _frame_reader(*, max_frame_bytes: int) -> _FrameReader:
    return ipc_module._FrameReader(max_frame_bytes=max_frame_bytes)


def _health_update() -> HealthUpdate:
    return HealthUpdate(
        worker=WorkerHealth(
            site='main',
            worker_id='["site","main","main"]',
            spawn_instance='spawn-1',
            heartbeat_at=10.0,
            monitor_alive=True,
            thread_names=('source:events',),
            stuck_threads=(),
            breaker_open=False,
            breaker_reason=None,
            socket_buffer_requested=_MAX_FRAME_BYTES,
            socket_send_buffer=_MAX_FRAME_BYTES * 2,
            socket_receive_buffer=_MAX_FRAME_BYTES * 2,
        ),
        watchdogs=(),
        sources=(),
        groups=(),
        event_sockets=(
            EventSocketHealth(
                source='events',
                group=None,
                role='server',
                connected=True,
                requested=_MAX_FRAME_BYTES,
                send=_MAX_FRAME_BYTES * 2,
                receive=_MAX_FRAME_BYTES * 2,
                queue_event_count=2,
                queue_byte_count=200,
                reason=None,
            ),
        ),
    )


def _complete_health_update() -> HealthUpdate:
    source = SourceHealth(
        site='main',
        source='events',
        source_generation='source-1',
        status='running',
        frontier=99,
        successful_clock=10.0,
        retry_attempt=0,
        next_retry_secs=None,
        open_range_count=0,
        late_id_count=0,
        last_error=None,
        phase='wait',
        heartbeat_at=10.0,
        heartbeat_age=0.0,
        deadline_at=15.0,
        overdue_reason=None,
    )
    group = GroupHealth(
        site='main',
        source='events',
        group='editorial',
        worker_instance='spawn-1',
        execution='sync',
        ordering='strict',
        phase='idle',
        status='running',
        cursor=99,
        current_event_id=None,
        current_plugin_id=None,
        current_callback_id=None,
        delivery_attempt=None,
        heartbeat_at=10.0,
        heartbeat_age=0.0,
        deadline_at=15.0,
        overdue_reason=None,
        queue_event_count=0,
        queue_byte_count=0,
        catchup_frontier=None,
        pending_catchup_epoch_count=0,
        oldest_catchup_epoch_age=None,
        join_phase='attached',
        outstanding_late_obligation_count=0,
        outstanding_late_id_count=0,
        outstanding_late_ids=(),
        outstanding_late_ids_truncated=False,
        parked=False,
        park_reason=None,
        failure_kind=None,
        failure_text=None,
        checkpoint_state='complete',
        conditions=(),
    )
    watchdogs = (
        WatchdogHealth(
            kind='source',
            site='main',
            source='events',
            group=None,
            worker_instance='spawn-1',
            phase='provider-query',
            heartbeat_at=10.0,
            deadline_at=20.0,
        ),
        WatchdogHealth(
            kind='lane',
            site='main',
            source='events',
            group='editorial',
            worker_instance='spawn-1',
            phase='callback',
            heartbeat_at=11.0,
            deadline_at=21.0,
            event_id=99,
            plugin_id='plugin-a',
            callback_id='callback-a',
            delivery_attempt=2,
        ),
        WatchdogHealth(
            kind='operator',
            site='main',
            source='events',
            group='editorial',
            worker_instance='spawn-1',
            phase='internal',
            heartbeat_at=12.0,
            deadline_at=22.0,
            operator_command='park',
        ),
    )
    return replace(
        _health_update(),
        watchdogs=watchdogs,
        sources=(source,),
        groups=(group,),
    )


def _invoke_join_lifecycle(
    client: EventClient,
    operation: str,
    identity: LiveJoinIdentity,
) -> None:
    if operation == 'abort':
        client.abort(JoinAbort(identity))
    elif operation == 'acknowledge':
        client.acknowledge_strict_control(StrictCapacityControl(identity=identity, abort_limit=4))
    elif operation == 'confirm':
        client.confirm(JoinConfirm(identity))
    else:
        client.finalize(JoinFinalize(identity))


def _join_enum_payload(decoder: str) -> dict[str, object]:
    identity: dict[str, object] = {
        'site': 'main',
        'source': 'events',
        'group': 'group-a',
        'join_kind': 'strict',
        'group_generation': 1,
        'attempt_token': _ATTEMPT_ID,
    }
    if decoder == '_decode_attempt':
        return identity
    identity.update(boundary=20, source_generation='source-1')
    if decoder == '_wire_identity':
        return identity
    payload: dict[str, object] = {
        'identity': identity,
        'action': 'halt',
        'delivery_floor': 20,
        'last_delivered_id': 20,
        'event_id': 21,
    }
    if decoder == '_decode_join_control':
        payload.update(control_kind='ordering', request_id=1)
    return payload


def _enum_payload(decoder: str) -> dict[str, object]:
    if decoder in {
        '_decode_attempt',
        '_wire_identity',
        '_decode_join_control',
        '_decode_strict_ordering',
    }:
        return _join_enum_payload(decoder)
    if decoder == '_decode_auth_body':
        return cast('dict[str, object]', json.loads(_event_authentication_frame('event')[4:]))
    if decoder == '_decode_bootstrap_request':
        return {
            'source': 'events',
            'group': 'group-a',
            'group_generation': 1,
            'start_mode': 'initialize',
        }
    raise AssertionError(decoder)


def _new_text_health_update() -> HealthUpdate:
    update = _health_update()
    source = SourceHealth(
        site='main',
        source='events',
        source_generation='source-1',
        status='running',
        frontier=99,
        successful_clock=10.0,
        retry_attempt=0,
        next_retry_secs=None,
        open_range_count=0,
        late_id_count=0,
        last_error=None,
        overdue_reason='source-phase-overdue',
    )
    group = GroupHealth(
        site='main',
        source='events',
        group='editorial',
        worker_instance='spawn-1',
        execution='sync',
        ordering='strict',
        phase='idle',
        status='running',
        cursor=99,
        current_event_id=None,
        current_plugin_id=None,
        current_callback_id=None,
        delivery_attempt=None,
        heartbeat_at=10.0,
        heartbeat_age=0.0,
        queue_event_count=0,
        queue_byte_count=0,
        catchup_frontier=None,
        pending_catchup_epoch_count=0,
        oldest_catchup_epoch_age=None,
        join_phase='attached',
        outstanding_late_obligation_count=0,
        outstanding_late_id_count=0,
        outstanding_late_ids=(),
        outstanding_late_ids_truncated=False,
        parked=False,
        park_reason=None,
        failure_kind=None,
        failure_text=None,
        checkpoint_state='complete',
        conditions=(),
        overdue_reason='lane-phase-overdue',
    )
    watchdog = WatchdogHealth(
        kind='operator',
        site='main',
        source='events',
        group='editorial',
        worker_instance='spawn-1',
        phase='internal',
        heartbeat_at=10.0,
        deadline_at=20.0,
        plugin_id='plugin-a',
        callback_id='callback-a',
        operator_command='park',
    )
    return replace(
        update,
        worker=replace(
            update.worker,
            watchdog_failure='watchdog-sampling-failed',
        ),
        watchdogs=(watchdog,),
        sources=(source,),
        groups=(group,),
    )


def _offer_joined_messages(
    channel: GroupChannelInterface,
    ordering: Literal['eager', 'strict'],
    first: EventDelivery,
    second: EventDelivery,
) -> None:
    offer = channel.offer_paced if ordering == 'strict' else channel.offer
    assert offer(first)
    assert offer(second)


@contextmanager
def _paired_event_client(
    path: Path, *, queue_max_entries: int = 2
) -> Generator[tuple[EventClient, socket.socket, GroupChannel]]:
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=queue_max_entries,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=1.0,
        stop=threading.Event(),
    )
    channel = GroupChannel(
        group='group-a', ordering='eager', max_events=queue_max_entries, max_bytes=_QUEUE_BYTES
    )
    reader, writer = socket.socketpair()
    reader.settimeout(0.0)
    selector = selectors.DefaultSelector()
    selector.register(reader, selectors.EVENT_READ)
    vars(client).update(_socket=reader, _selector=selector, _join_channel=channel)
    try:
        yield client, writer, channel
    finally:
        client.close()
        writer.close()


def _install_grant(
    server: EventServer,
    group: str,
    *,
    spawn_instance: str = 'spawn-1',
    capability: str = 'capability-1',
) -> None:
    server.install_grant(
        ipc_module.EventPeerGrant(
            source='events',
            group=group,
            dependent_spawn_instance=spawn_instance,
            capability=capability,
        )
    )


def _join_client(
    server: EventServer,
    client: EventClient,
    *,
    group: str = 'group-a',
    ordering: Literal['eager', 'strict'] = 'eager',
    max_events: int = 2,
) -> tuple[GroupChannel, GroupChannelInterface]:
    channels: list[GroupChannelInterface] = []
    attempt = JoinAttemptIdentity(
        site='main',
        source='events',
        group=group,
        join_kind=ordering,
        group_generation=1,
        attempt_token=_ATTEMPT_ID,
    )
    identity = LiveJoinIdentity(
        site='main',
        source='events',
        group=group,
        join_kind=ordering,
        source_generation='source-1',
        group_generation=1,
        boundary=0,
        attempt_token=_ATTEMPT_ID,
    )

    class Source(_JoinSourceStub):
        def request(self, request: JoinRequest, channel: GroupChannelInterface) -> JoinTarget:
            assert request == JoinRequest(attempt)
            channels.append(channel)
            return JoinTarget(identity)

        def event_channel_lost(self, group: str, *, reason: str) -> None:
            del group, reason

    channel = GroupChannel(
        group=group,
        ordering=ordering,
        max_events=max_events,
        max_bytes=_QUEUE_BYTES,
    )
    source_thread = _SourceThreadHarness(server, Source())
    try:
        source_thread._start()
        assert client.request(JoinRequest(attempt), channel) == JoinTarget(identity)
    finally:
        source_thread._close()
    (remote,) = channels
    assert remote.status.attached
    return channel, remote


def _partial_writer(*, max_bytes: int | None = None) -> _PartialWriter:
    return ipc_module._PartialWriter(max_bytes=max_bytes)


def _pump(
    server: EventServer,
    clients: tuple[EventClient, ...],
    channels: tuple[GroupChannelInterface, ...],
) -> None:
    for _ in range(100):
        server.poll(0.001)
        for client in clients:
            client.poll(0.001)
        if all(channel.status.queued_count for channel in channels):
            return
    raise AssertionError('event IPC did not make progress')


def _retained_connection_identity_count(server: EventServer) -> int:
    return len(server._connection_fences)


def _run_bounded_client_call(
    client: EventClient,
    call: Callable[[], object],
) -> object | Exception:
    complete = threading.Event()
    results: list[object | Exception] = []

    def invoke() -> None:
        try:
            results.append(call())
        except (ConnectionError, JoinProtocolError) as error:
            results.append(error)
        finally:
            complete.set()

    thread = threading.Thread(target=invoke)
    thread.start()
    try:
        assert complete.wait(1.0)
    finally:
        if not complete.is_set():
            client.close()
        thread.join(1.0)
    assert not thread.is_alive()
    assert len(results) == 1
    return results[0]


def _script_peer_recv(
    peer: ipc_module._ServerPeer,
    *outcomes: bytes | OSError,
) -> None:
    sock = peer._socket
    vars(peer)['_socket'] = cast(
        'socket.socket',
        _ScriptedRecvSocket(sock, outcomes),
    )


def _server_coordination_socket(server: EventServer) -> socket.socket:
    peers = server._peers
    return next(peer._socket for peer in peers.values() if peer._role == 'coordination')


def _server_pair_connection_id(server: EventServer, group: str) -> str | None:
    peers = tuple(peer for peer in server._peers.values() if peer._group == group)
    if len(peers) != 2 or {peer._role for peer in peers} != {
        'coordination',
        'event',
    }:
        return None
    connection_ids = {peer._connection_id for peer in peers}
    if len(connection_ids) != 1:
        return None
    return connection_ids.pop()


def _server_peer(
    server: EventServer,
    role: Literal['coordination', 'event'] | None,
) -> ipc_module._ServerPeer:
    return next(peer for peer in server._peers.values() if peer._role == role)


def _service_event_server_until(
    server: EventServer,
    predicate: Callable[[], bool],
    *,
    source: _JoinSourceStub | None = None,
) -> None:
    for _ in range(200):
        server.poll(0.001)
        if source is not None:
            server.dispatch_join_requests(source)
        if predicate():
            return
    raise AssertionError('event server did not reach expected state')


def _socket_path(tmp_path: Path) -> Path:
    assert len(_SHORT_SOCKET_ROOT) == 1
    return _SHORT_SOCKET_ROOT[0] / f'{tmp_path.name}.sock'


def _source_owner_guard(
    original_modify_peer: Callable[[EventServer, object], None],
    owner_ident: Callable[[], int | None],
) -> Callable[[EventServer, object], None]:

    def require_source_owner_thread(
        current_server: EventServer,
        peer: object,
    ) -> None:
        assert threading.get_ident() == owner_ident()
        original_modify_peer(current_server, peer)

    return require_source_owner_thread


def _wait_for_channel_count(
    client: EventClient,
    channel: GroupChannelInterface,
    expected: int,
) -> None:
    for _ in range(100):
        client.poll(0.001)
        if channel.status.queued_count == expected:
            return
    raise AssertionError('event channel did not reach expected count')


def _wait_for_client_state(
    server: EventServer,
    client: EventClient,
    *,
    connected: bool,
) -> None:
    for _ in range(100):
        server.poll(0.001)
        client.poll(0.001)
        if client.connected == connected and (not connected or bool(server._connected_groups)):
            return
    raise AssertionError('event client did not reach expected state')


def _wait_for_server_pair_replacement(
    server: EventServer,
    group: str,
    previous_connection_id: str,
) -> str:
    for _ in range(200):
        server.poll(0.001)
        connection_id = _server_pair_connection_id(server, group)
        if connection_id is not None and connection_id != previous_connection_id:
            return connection_id
    raise AssertionError('event server did not authenticate a replacement pair')


def _write_event_server_stream(
    server: EventServer,
    sock: socket.socket,
    data: bytes,
) -> None:
    offset = 0
    for _ in range(1_000):
        try:
            written = sock.send(memoryview(data)[offset:])
        except BlockingIOError:
            written = 0
        if written < 0:
            raise AssertionError('event stream write made negative progress')
        offset += written
        server.poll(0.001)
        if offset == len(data):
            return
    raise AssertionError('event stream write did not complete')


def test_body_free_event_messages_charge_complete_wire_bytes() -> None:
    identity = LiveJoinIdentity(
        site='main',
        source='events',
        group='group-a',
        join_kind='strict',
        source_generation='source-1',
        group_generation=1,
        boundary=20,
        attempt_token=_ATTEMPT_ID,
    )
    messages = (
        StrictCapacityControl(identity=identity, abort_limit=4),
        StrictOrderingControl(
            identity=identity,
            event_id=21,
            last_delivered_id=20,
            delivery_floor=20,
            action='halt',
        ),
    )

    for message in messages:
        frame = _encode_fanout_message(message)
        wire_bytes = len(_frame_bytes(frame))
        decoded = _decode_event_wire(_frame_bytes(frame))
        assert isinstance(
            decoded,
            StrictCapacityControl | StrictOrderingControl,
        )
        channel = GroupChannel(
            group='group-a',
            ordering='strict',
            max_events=1,
            max_bytes=wire_bytes - 1,
        )

        assert message.byte_count == wire_bytes
        assert decoded.byte_count == wire_bytes
        assert not channel.offer(message)
        assert channel.status.reason == 'queue-byte-exhausted'


def test_client_delivers_range_changed_without_detaching_a_full_lane(tmp_path: Path) -> None:
    client = _event_client(_socket_path(tmp_path))
    channel = GroupChannel(group='group-a', ordering='eager', max_events=1, max_bytes=_QUEUE_BYTES)
    first = EventDelivery(EncodedEvent.from_event_frame(7, encode_event(_event(7))))
    assert channel.offer(first)
    vars(client)['_join_channel'] = channel
    assert client._queue_decoded((fanout_module.RangeChanged(),)) == 1
    assert channel.status.attached
    assert channel.drain() == (first, fanout_module.RangeChanged())


def test_client_reconnect_drops_stale_queue(tmp_path: Path) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )
    with server:
        _install_grant(server, 'group-a')
        client.connect()
        for _ in range(100):
            server.poll(0.001)
            if server._connected_groups:
                break
        channel, remote = _join_client(server, client, max_events=2)
        remote.offer(EventDelivery(EncodedEvent.from_event_frame(4, encode_event(_event(4)))))
        _pump(server, (client,), (channel,))
        assert channel.status.queued_count == 1
        client.reconnect()
        assert channel.status.queued_count == 0
        assert client._needs_catchup
        assert not channel.status.attached
        assert channel.status.queued_bytes == 0
        successor, current = _join_client(server, client)
        stale = EventDelivery(EncodedEvent.from_event_frame(5, encode_event(_event(5))))
        fresh = EventDelivery(EncodedEvent.from_event_frame(6, encode_event(_event(6))))
        assert not remote.offer(stale)
        assert not remote.offer_paced(stale)
        remote.detach('stale-owner-closed')
        assert current.offer(fresh)
        _pump(server, (client,), (successor,))
        assert successor.drain() == (fresh,)
        assert current.status.attached
    client.close()


def test_compact_health_control_encoding_expands_bounded_nul_diagnostic() -> None:
    diagnostic = '\0' * 4_096
    update = _health_update()
    update = replace(
        update,
        worker=replace(update.worker, breaker_reason=diagnostic),
    )
    encoded = encode_control_message(update)

    assert len(diagnostic.encode()) == 4_096
    assert encoded.count(b'\\u0000') == 4_096


def test_compact_health_control_limit_excludes_only_outer_length_prefix() -> None:
    update = _health_update()
    body_bytes = len(encode_control_message(update)) - 4
    exact_parent_socket, exact_child_socket = socket.socketpair()
    exact_parent = WorkerControl(
        exact_parent_socket, max_frame_bytes=_MAX_FRAME_BYTES, send_deadline_secs=1.0
    )
    exact_child = WorkerControl(
        exact_child_socket, max_frame_bytes=_MAX_FRAME_BYTES, send_deadline_secs=1.0
    )
    small_parent_socket, small_child_socket = socket.socketpair()
    small_parent = WorkerControl(
        small_parent_socket, max_frame_bytes=_MAX_FRAME_BYTES, send_deadline_secs=1.0
    )
    small_child = WorkerControl(
        small_child_socket, max_frame_bytes=_MAX_FRAME_BYTES, send_deadline_secs=1.0
    )
    try:
        vars(exact_parent)['_max_frame_bytes'] = body_bytes
        vars(small_parent)['_max_frame_bytes'] = body_bytes - 1
        exact_parent.send(update)
        assert exact_child.receive(timeout=1.0) == update
        with pytest.raises(_FrameError, match='exceeds'):
            small_parent.send(update)
    finally:
        exact_parent.close()
        exact_child.close()
        small_parent.close()
        small_child.close()


def test_connect_preserves_initiating_failure_and_closes_every_acquisition(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    attempts: list[str] = []
    initiating = ValueError('coordination registration failed')

    class Socket:
        def __init__(self, name: str) -> None:
            self.name = name

        def settimeout(self, _timeout: float) -> None:
            pass

        def connect(self, _path: str) -> None:
            pass

        def close(self) -> None:
            attempts.append(self.name)

    class Selector:
        def __init__(self, name: str) -> None:
            self.name = name

        def register(self, *_args: object) -> None:
            if self.name == 'coordination-selector':
                raise initiating

        def close(self) -> None:
            attempts.append(self.name)
            raise OSError(self.name + ' cleanup failed')

    sockets = iter((Socket('event'), Socket('coordination')))
    selectors = iter((Selector('event-selector'), Selector('coordination-selector')))
    monkeypatch.setattr(ipc_module.socket, 'socket', Mock(side_effect=sockets))
    monkeypatch.setattr(ipc_module.selectors, 'DefaultSelector', lambda: next(selectors))
    monkeypatch.setattr(ipc_module, '_request_socket_buffers', Mock(return_value=None))
    monkeypatch.setattr(EventClient, '_send_authentication', Mock(return_value=None))
    client = EventClient(
        path=tmp_path / 'events.sock',
        source='events',
        group='remote',
        spawn_instance='spawn',
        capability='capability',
        max_frame_bytes=131_072,
        queue_max_entries=8,
        queue_max_bytes=262_144,
        coordination_deadline_secs=1.0,
        stop=threading.Event(),
    )
    with pytest.raises(ValueError, match='failed') as caught:
        client.connect()
    assert caught.value is initiating
    assert attempts == ['event-selector', 'coordination-selector', 'event', 'coordination']
    assert len(initiating.__notes__) == 2
    assert not client.connected


def test_control_and_event_protocols_are_not_interchangeable() -> None:
    delivery = EventDelivery(EncodedEvent.from_event_frame(1, encode_event(_event(1))))

    with pytest.raises(_FrameError):
        _decode_control_message(_frame_bytes(_encode_fanout_message(delivery)))
    with pytest.raises(_FrameError):
        _decode_event_wire(encode_control_message(StopCommand(reason='stop')))


def test_control_messages_are_closed_versioned_and_secret_safe() -> None:
    messages = (
        ControlHello(
            spawn_instance='spawn-1',
            worker_id='["site","main","main"]',
            pid=123,
        ),
        StopCommand(reason='operator'),
        OperatorCommand(
            request_id='operator-request-1',
            command='park',
            arguments={
                'site': 'main',
                'group': 'editorial',
                'reason': 'maintenance',
            },
            identity=AuditIdentity(local_uid=501, username='operator', pid=123),
        ),
        CommandResult(
            command='stop',
            ok=True,
            message='accepted',
            request_id='operator-request-1',
            result={'accepted': True},
        ),
        _health_update(),
    )

    for message in messages:
        assert _decode_control_message(encode_control_message(message)) == message

    encoded = bytearray(encode_control_message(messages[0]))
    encoded[-2] = ord('9')
    with pytest.raises(_FrameError):
        _decode_control_message(bytes(encoded))
    health = _health_update()
    redacted_health = HealthUpdate(
        worker=replace(
            health.worker,
            breaker_reason='token=must-not-cross-ipc',
        ),
        watchdogs=health.watchdogs,
        sources=health.sources,
        groups=health.groups,
        event_sockets=health.event_sockets,
    )
    assert redacted_health.worker.breaker_reason == '<redacted>'
    assert _decode_control_message(encode_control_message(redacted_health)) == redacted_health


def test_coordination_channel_rejects_fanout_before_decoding_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _event_client(Path('/unused.sock'))
    delivery = EventDelivery(EncodedEvent.from_event_frame(61, encode_event(_event(61))))
    frame = _frame_bytes(_encode_fanout_message(delivery))
    decode = Mock(side_effect=AssertionError('event body was decoded'))
    monkeypatch.setattr(ipc_module, 'decode_event', decode)

    with pytest.raises(_FrameError, match='coordination stream received a fanout frame'):
        client._coordination_reader._feed(frame)

    decode.assert_not_called()


def test_detach_frame_charges_complete_wire_bytes_at_exact_limit() -> None:
    detach_frame = ipc_module._detach_frame
    frame = detach_frame('queue-closed')
    wire_bytes = len(_frame_bytes(frame))
    exact_writer = _partial_writer(max_bytes=wire_bytes)
    short_writer = _partial_writer(max_bytes=wire_bytes - 1)

    assert exact_writer._append(frame)
    assert not short_writer._append(frame)
    assert short_writer._rejection_reason == 'queue-byte-exhausted'
    assert frame._charge_bytes == wire_bytes


def test_event_client_connect_deadline_closes_both_private_sockets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_socket = socket.socket
    created: list[_StalledConnectSocket] = []

    def socket_factory(family: int, kind: int) -> _StalledConnectSocket:
        wrapped = _StalledConnectSocket(real_socket(family, kind))
        created.append(wrapped)
        return wrapped

    monkeypatch.setattr(ipc_module.socket, 'socket', socket_factory)
    client = EventClient(
        path=_socket_path(tmp_path),
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=0.05,
        stop=threading.Event(),
    )

    started = time.monotonic()
    with pytest.raises(ConnectionError, match='event endpoint connection failed'):
        client.connect()
    elapsed = time.monotonic() - started

    state = vars(client)
    assert 0.04 <= elapsed < 1.0
    assert len(created) == 2
    assert all(sock.fileno() == -1 for sock in created)
    assert state['_socket'] is None
    assert state['_coordination_socket'] is None
    assert state['_selector'] is None
    assert state['_coordination_selector'] is None
    assert client.connected is False


def test_event_client_detach_preserves_additional_cleanup_note(tmp_path: Path) -> None:
    first = OSError('selector close failed')
    later = RuntimeError('channel detach failed')
    client = EventClient(
        path=tmp_path / 'events.sock',
        source='events',
        group='remote',
        spawn_instance='spawn',
        capability='capability',
        max_frame_bytes=131_072,
        queue_max_entries=8,
        queue_max_bytes=262_144,
        coordination_deadline_secs=1.0,
        stop=threading.Event(),
    )
    vars(client)['_selector'] = SimpleNamespace(close=Mock(side_effect=first))
    vars(client)['_join_channel'] = SimpleNamespace(detach=Mock(side_effect=later))

    with pytest.raises(OSError, match='selector close failed') as caught:
        client.close()

    assert caught.value is first
    assert first.__notes__ == ["additional cleanup failure: RuntimeError('channel detach failed')"]


def test_event_client_poll_handles_partial_prefix_and_body(tmp_path: Path) -> None:
    message = EventDelivery(EncodedEvent.from_event_frame(1, encode_event(_event(1))))
    frame = _encode_fanout_message(message)
    wire = b''.join(frame._parts)
    halfway = 4 + (len(wire) - 4) // 2
    with _paired_event_client(tmp_path / 'unused.sock') as (client, writer, channel):
        writer.sendall(wire[:2])
        assert client.poll(0.01) == 0
        writer.sendall(wire[2:halfway])
        assert client.poll(0.01) == 0
        writer.sendall(wire[halfway:])
        assert client.poll(0.01) == 1
        assert channel.drain() == (message,)


def test_event_client_poll_reads_whole_frame_in_one_call(tmp_path: Path) -> None:
    first = EventDelivery(EncodedEvent.from_event_frame(1, encode_event(_event(1))))
    second = EventDelivery(EncodedEvent.from_event_frame(2, encode_event(_event(2))))
    wire = b''.join(
        part for message in (first, second) for part in _encode_fanout_message(message)._parts
    )
    with _paired_event_client(tmp_path / 'unused.sock') as (client, writer, channel):
        writer.sendall(wire)
        assert client.poll(0.01) == 1
        assert channel.drain() == (first,)
        assert client.poll(0.01) == 1
        assert channel.drain() == (second,)


def test_event_client_poll_stops_reading_body_when_queue_is_full(tmp_path: Path) -> None:
    first = EventDelivery(EncodedEvent.from_event_frame(1, encode_event(_event(1))))
    second = EventDelivery(EncodedEvent.from_event_frame(2, encode_event(_event(2))))
    first_wire = b''.join(_encode_fanout_message(first)._parts)
    second_wire = b''.join(_encode_fanout_message(second)._parts)
    with _paired_event_client(tmp_path / 'unused.sock', queue_max_entries=1) as (
        client,
        writer,
        channel,
    ):
        writer.sendall(first_wire + second_wire)
        assert client.poll(0.01) == 1
        assert client.poll(0.01) == 0
        reader = client._socket
        assert reader is not None
        assert reader.recv(4, socket.MSG_PEEK) == second_wire[:4]
        assert channel.drain() == (first,)
        assert client.poll(0.01) == 1
        assert channel.drain() == (second,)


def test_event_client_selector_failure_closes_private_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_socket = socket.socket
    sockets = (
        _ScriptedStartupSocket(real_socket(socket.AF_UNIX, socket.SOCK_STREAM)),
        _ScriptedStartupSocket(real_socket(socket.AF_UNIX, socket.SOCK_STREAM)),
    )
    scripted_sockets = deque(sockets)
    selectors_created = (
        _StartupSelector(fail_registration=False),
        _StartupSelector(fail_registration=True),
    )
    scripted_selectors = deque(selectors_created)

    def socket_factory(_family: int, _kind: int) -> _ScriptedStartupSocket:
        return scripted_sockets.popleft()

    def selector_factory() -> _StartupSelector:
        return scripted_selectors.popleft()

    monkeypatch.setattr(ipc_module.socket, 'socket', socket_factory)
    monkeypatch.setattr(ipc_module.selectors, 'DefaultSelector', selector_factory)
    client = EventClient(
        path=_socket_path(tmp_path),
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=0.1,
        stop=threading.Event(),
    )

    with pytest.raises(ConnectionError, match='event endpoint connection failed'):
        client.connect()

    state = vars(client)
    assert selectors_created[0]._registered
    assert selectors_created[1]._registration_attempted
    assert not selectors_created[1]._registered
    assert all(selector._closed for selector in selectors_created)
    assert all(sock.fileno() == -1 for sock in sockets)
    assert state['_socket'] is None
    assert state['_coordination_socket'] is None
    assert state['_selector'] is None
    assert state['_coordination_selector'] is None
    assert client.connected is False


def test_event_client_socket_creation_failure_closes_created_socket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_socket = socket.socket
    created: list[socket.socket] = []

    def socket_factory(family: int, kind: int) -> socket.socket:
        if created:
            raise OSError(errno.EMFILE, 'scripted descriptor exhaustion')
        sock = real_socket(family, kind)
        created.append(sock)
        return sock

    monkeypatch.setattr(ipc_module.socket, 'socket', socket_factory)
    client = EventClient(
        path=_socket_path(tmp_path),
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=0.05,
        stop=threading.Event(),
    )

    try:
        with pytest.raises(ConnectionError, match='event endpoint connection failed'):
            client.connect()

        state = vars(client)
        assert len(created) == 1
        assert created[0].fileno() == -1
        assert state['_socket'] is None
        assert state['_coordination_socket'] is None
        assert state['_selector'] is None
        assert state['_coordination_selector'] is None
        assert client.connected is False
    finally:
        if created and created[0].fileno() != -1:
            created[0].close()


def test_event_client_zero_authentication_send_closes_both_private_sockets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_socket = socket.socket

    def _send_zero(_value: memoryview, _timeout: float | None) -> int:
        return 0

    sockets = (
        _ScriptedStartupSocket(
            real_socket(socket.AF_UNIX, socket.SOCK_STREAM),
            send_action=_send_zero,
        ),
        _ScriptedStartupSocket(real_socket(socket.AF_UNIX, socket.SOCK_STREAM)),
    )
    scripted = deque(sockets)

    def socket_factory(_family: int, _kind: int) -> _ScriptedStartupSocket:
        return scripted.popleft()

    monkeypatch.setattr(ipc_module.socket, 'socket', socket_factory)
    client = EventClient(
        path=_socket_path(tmp_path),
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=0.1,
        stop=threading.Event(),
    )

    with pytest.raises(ConnectionError, match='event endpoint connection failed'):
        client.connect()

    state = vars(client)
    assert all(sock.fileno() == -1 for sock in sockets)
    assert state['_socket'] is None
    assert state['_coordination_socket'] is None
    assert state['_selector'] is None
    assert state['_coordination_selector'] is None
    assert client.connected is False


def test_event_codec_reuses_encoded_body_and_preserves_frame_order() -> None:
    encoded = EncodedEvent.from_event_frame(21, encode_event(_event(21)))
    control = StrictCapacityControl(
        identity=LiveJoinIdentity(
            site='main',
            source='events',
            group='group-a',
            join_kind='strict',
            source_generation='source-1',
            group_generation=1,
            boundary=20,
            attempt_token=_ATTEMPT_ID,
        ),
        abort_limit=4,
    )
    delivery = EventDelivery(encoded)

    control_frame = _encode_fanout_message(control)
    event_frame = _encode_fanout_message(delivery)
    assert event_frame._payload is encoded.event_frame
    assert _decode_event_wire(_frame_bytes(control_frame)) == control
    decoded_delivery = _decode_event_wire(_frame_bytes(event_frame))
    assert isinstance(decoded_delivery, EventDelivery)
    assert decoded_delivery.encoded.event_id == 21
    assert decode_event(decoded_delivery.encoded.event_frame) == _event(21)

    reader = _frame_reader(max_frame_bytes=8_388_608)
    stream = _frame_bytes(control_frame) + _frame_bytes(event_frame)
    assert reader._feed(stream[:7]) == ()
    decoded = reader._feed(stream[7:])
    assert decoded == (control, decoded_delivery)


def test_event_connection_fence_is_removed_when_grant_is_replaced(tmp_path: Path) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    )

    def _client(
        group: str,
        *,
        spawn_instance: str = 'dependent-1',
        capability: str,
    ) -> EventClient:
        return EventClient(
            path=path,
            source='events',
            group=group,
            spawn_instance=spawn_instance,
            capability=capability,
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=2,
            queue_max_bytes=_QUEUE_BYTES,
            coordination_deadline_secs=30.0,
            stop=threading.Event(),
        )

    group_a = _client('group-a', capability='capability-a')
    old_group_b = _client('group-b', capability='capability-b')
    old_capability = _client('group-b', capability='capability-b')
    replacement = _client(
        'group-b',
        spawn_instance='dependent-2',
        capability='capability-2',
    )
    with server:
        _install_grant(
            server,
            'group-a',
            spawn_instance='dependent-1',
            capability='capability-a',
        )
        _install_grant(
            server,
            'group-b',
            spawn_instance='dependent-1',
            capability='capability-b',
        )
        group_a.connect()
        _wait_for_client_state(server, group_a, connected=True)
        old_group_b.connect()
        _service_event_server_until(
            server,
            lambda: server._connected_groups == ('group-a', 'group-b'),
        )
        assert set(vars(server)['_connection_fences']) == {'group-a', 'group-b'}

        server.install_grant(
            ipc_module.EventPeerGrant(
                source='events',
                group='group-a',
                dependent_spawn_instance='dependent-2',
                capability='capability-2',
            )
        )
        assert set(vars(server)['_connection_fences']) == {'group-b'}

        ready = server.install_grant(
            ipc_module.EventPeerGrant(
                source='events',
                group='group-b',
                dependent_spawn_instance='dependent-2',
                capability='capability-2',
            )
        )
        assert set(vars(server)['_connection_fences']) == set()
        assert ready == ipc_module.EventPeerReady(
            source='events',
            group='group-b',
            dependent_spawn_instance='dependent-2',
        )

        replacement.connect()
        _wait_for_client_state(server, replacement, connected=True)
        assert set(vars(server)['_connection_fences']) == {'group-b'}
        fences = server._connection_fences
        fence_state = cast('dict[str, object]', vars(fences['group-b'])).copy()

        old_capability.connect()
        _wait_for_client_state(server, old_capability, connected=False)
        assert not old_capability.connected
        fences = server._connection_fences
        assert cast('dict[str, object]', vars(fences['group-b'])) == fence_state
        assert replacement.connected
        assert server._connected_groups == ('group-b',)

    for peer in (group_a, old_group_b, old_capability, replacement):
        peer.close()


def test_event_connection_fence_stays_bounded_and_rejects_ancient_delayed_role(
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )

    with (
        socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as old_event,
        socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as old_coordination,
        server,
    ):
        _install_grant(server, 'group-a')
        old_event.connect(os.fspath(path))
        old_event.settimeout(0.0)
        old_coordination.connect(os.fspath(path))
        old_coordination.settimeout(0.0)
        _write_event_server_stream(
            server,
            old_event,
            _event_authentication_frame('event', connection_id='old-connection'),
        )
        _service_event_server_until(
            server,
            lambda: any(
                vars(peer)['_connection_id'] == 'old-connection' for peer in server._peers.values()
            ),
        )

        client.connect()
        _wait_for_client_state(server, client, connected=True)
        previous_connection_id = _server_pair_connection_id(server, 'group-a')
        assert previous_connection_id is not None
        connection_ids = {previous_connection_id}
        for _ in range(128):
            client.reconnect()
            current_connection_id = _wait_for_server_pair_replacement(
                server,
                'group-a',
                previous_connection_id,
            )
            assert current_connection_id not in connection_ids
            connection_ids.add(current_connection_id)
            previous_connection_id = current_connection_id

        assert len(connection_ids) == 129

        assert _retained_connection_identity_count(server) == 1
        authenticated = tuple(
            peer for peer in server._peers.values() if vars(peer)['_group'] == 'group-a'
        )
        assert len(authenticated) == 2
        assert {vars(peer)['_role'] for peer in authenticated} == {'coordination', 'event'}
        assert len({vars(peer)['_connection_id'] for peer in authenticated}) == 1

        _write_event_server_stream(
            server,
            old_coordination,
            _event_authentication_frame('coordination', connection_id='old-connection'),
        )
        ancient_closed = False
        for _ in range(200):
            server.poll(0.001)
            client.poll(0.001)
            try:
                ancient_closed = old_coordination.recv(1) == b''
            except BlockingIOError:
                continue
            if ancient_closed:
                break

        assert ancient_closed
        assert client.connected
        assert server._connected_groups == ('group-a',)
    client.close()


def test_event_coordination_rpc_caller_stop_wins_before_timeout(
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    stop = threading.Event()
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='dependent-1',
        capability='capability-a',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=30.0,
        stop=stop,
    )
    request = GroupBootstrapRequest(
        source='events',
        group='group-a',
        group_generation=7,
        start_mode='initialize',
    )

    with server:
        _install_grant(
            server,
            'group-a',
            spawn_instance='dependent-1',
            capability='capability-a',
        )
        client.connect()
        _wait_for_client_state(server, client, connected=True)
        vars(client)['_coordination_deadline_secs'] = 0.0
        stop.set()
        result = _run_bounded_client_call(client, lambda: client.observe_group(request))

    assert isinstance(result, ConnectionError)
    assert str(result) == 'event channel stopped during join request'
    assert client.detach_reason == 'coordination-stop'


def test_event_coordination_rpc_finishes_after_real_socket_short_writes(
    tmp_path: Path,
) -> None:
    max_frame_bytes = 1_048_576
    path = _socket_path(tmp_path)
    request = JoinConfirm(
        LiveJoinIdentity(
            site='main',
            source='events',
            group='group-a',
            join_kind='eager',
            source_generation='source-1',
            group_generation=1,
            boundary=100,
            attempt_token='x' * 90_000,
        )
    )
    expected = Attached(request.identity)
    received: list[JoinConfirm] = []
    losses: list[tuple[str, str]] = []

    class _Source(_JoinSourceStub):
        def event_channel_lost(self, group: str, *, reason: str) -> None:
            losses.append((group, reason))

        def confirm(self, confirmation: JoinConfirm) -> Attached:
            assert confirmation.identity.group == 'group-a'
            received.append(confirmation)
            return expected

    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=max_frame_bytes,
        queue_max_entries=8,
        queue_max_bytes=max_frame_bytes * 2,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='dependent-1',
        capability='capability-a',
        max_frame_bytes=max_frame_bytes,
        queue_max_entries=8,
        queue_max_bytes=max_frame_bytes * 2,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )
    complete = threading.Event()
    results: list[Attached | JoinRetry | Exception] = []
    source_thread: _SourceThreadHarness | None = None

    def reconcile() -> None:
        try:
            results.append(client.confirm(request))
        except (ConnectionError, JoinProtocolError) as error:
            results.append(error)
        finally:
            complete.set()

    with server:
        _install_grant(
            server,
            'group-a',
            spawn_instance='dependent-1',
            capability='capability-a',
        )
        client.connect()
        _wait_for_client_state(server, client, connected=True)
        assert server._connected_groups == ('group-a',)
        assert _clamp_coordination_send_buffer(client, 4_096) < max_frame_bytes
        server_coordination = _server_coordination_socket(server)
        thread = threading.Thread(target=reconcile)
        thread.start()
        try:
            readable, _writable, _exceptional = select.select(
                (server_coordination,),
                (),
                (),
                1.0,
            )
            assert readable == [server_coordination]
            source_thread = _SourceThreadHarness(server, _Source())
            source_thread._start()
            assert complete.wait(2.0)
        finally:
            if not complete.is_set():
                client.close()
            thread.join(1.0)
            if source_thread is not None:
                source_thread._close()
            client.close()

    assert not thread.is_alive()
    assert results == [expected]
    assert received == [request]
    assert losses == []


def test_event_coordination_rpc_times_out_when_no_reply_arrives(
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    stop = threading.Event()
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='dependent-1',
        capability='capability-a',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=0.05,
        stop=stop,
    )
    request = GroupBootstrapRequest(
        source='events',
        group='group-a',
        group_generation=7,
        start_mode='initialize',
    )

    time.sleep(0.1)
    with server:
        _install_grant(
            server,
            'group-a',
            spawn_instance='dependent-1',
            capability='capability-a',
        )
        client.connect()
        _wait_for_client_state(server, client, connected=True)
        start = time.monotonic()
        result = _run_bounded_client_call(client, lambda: client.observe_group(request))
        elapsed = time.monotonic() - start

    assert isinstance(result, ConnectionError)
    assert str(result) == 'event coordination reply timed out'
    assert 0.04 <= elapsed < 0.5
    assert client.detach_reason == 'coordination-reply-timeout'


def test_event_frame_headers_match_golden_event_and_strict_control_bytes() -> None:
    delivery = EventDelivery(EncodedEvent.from_event_frame(21, b'event'))
    identity = LiveJoinIdentity(
        site='main',
        source='events',
        group='group-a',
        join_kind='strict',
        source_generation='source-1',
        group_generation=1,
        boundary=20,
        attempt_token=_ATTEMPT_ID,
    )
    strict = StrictCapacityControl(identity=identity, abort_limit=4)

    assert _encode_fanout_message(delivery)._header == (
        b'{"kind":"event","metadata":{"event_id":21},"schema":'
        b'"sg-event-daemon-event-channel","version":1}'
    )
    assert _encode_fanout_message(strict)._header == (
        b'{"kind":"strict-capacity","metadata":{"abort_limit":4,"identity":'
        b'{"attempt_token":"attempt-1","boundary":20,"group":"group-a",'
        b'"group_generation":1,"join_kind":"strict","site":"main",'
        b'"source":"events","source_generation":"source-1"}},"schema":'
        b'"sg-event-daemon-event-channel","version":1}'
    )


def test_event_grants_are_exact_reconnectable_and_replace_old_spawn(
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    first_grant = ipc_module.EventPeerGrant(
        source='events',
        group='group-a',
        dependent_spawn_instance='dependent-1',
        capability='capability-1',
    )
    replacement = ipc_module.EventPeerGrant(
        source='events',
        group='group-a',
        dependent_spawn_instance='dependent-2',
        capability='capability-2',
    )
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    )

    def _client(
        *,
        group: str,
        spawn_instance: str,
        capability: str,
    ) -> EventClient:
        return EventClient(
            path=path,
            source='events',
            group=group,
            spawn_instance=spawn_instance,
            capability=capability,
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=2,
            queue_max_bytes=_QUEUE_BYTES,
            coordination_deadline_secs=30.0,
            stop=threading.Event(),
        )

    unprovisioned = _client(
        group='group-a',
        spawn_instance='dependent-1',
        capability='capability-1',
    )
    borrowed = _client(
        group='group-b',
        spawn_instance='dependent-1',
        capability='capability-1',
    )
    current = _client(
        group='group-a',
        spawn_instance='dependent-1',
        capability='capability-1',
    )
    restarted = _client(
        group='group-a',
        spawn_instance='dependent-2',
        capability='capability-2',
    )

    with server:
        unprovisioned.connect()
        _wait_for_client_state(server, unprovisioned, connected=False)
        assert not unprovisioned.connected

        assert server.install_grant(first_grant) == ipc_module.EventPeerReady(
            source='events',
            group='group-a',
            dependent_spawn_instance='dependent-1',
        )
        borrowed.connect()
        _wait_for_client_state(server, borrowed, connected=False)
        assert not borrowed.connected

        current.connect()
        _wait_for_client_state(server, current, connected=True)
        assert server._connected_groups == ('group-a',)
        current.reconnect()
        _wait_for_client_state(server, current, connected=True)
        assert current.connected

        assert server.install_grant(replacement) == ipc_module.EventPeerReady(
            source='events',
            group='group-a',
            dependent_spawn_instance='dependent-2',
        )
        _wait_for_client_state(server, current, connected=False)
        assert not current.connected

        current.connect()
        _wait_for_client_state(server, current, connected=False)
        assert not current.connected
        restarted.connect()
        _wait_for_client_state(server, restarted, connected=True)
        assert server._connected_groups == ('group-a',)

    for peer in (unprovisioned, borrowed, current, restarted):
        peer.close()


def test_event_join_rpc_runs_on_source_thread_and_releases_on_eof(
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=8,
        queue_max_bytes=_QUEUE_BYTES,
    )
    grant = ipc_module.EventPeerGrant(
        source='events',
        group='group-a',
        dependent_spawn_instance='dependent-1',
        capability='capability-1',
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='dependent-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=8,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )
    identity = LiveJoinIdentity(
        site='main',
        source='events',
        group='group-a',
        join_kind='strict',
        source_generation='source-1',
        group_generation=1,
        boundary=20,
        attempt_token=_ATTEMPT_ID,
    )
    operations: list[tuple[str, int]] = []
    channel_lost = threading.Event()

    class _Source(_JoinSourceStub):
        def abort(self, abort: JoinAbort) -> None:
            del abort
            operations.append(('abort', threading.get_ident()))

        def acknowledge_strict_control(
            self,
            control: StrictCapacityControl | StrictOrderingControl,
        ) -> None:
            del control
            operations.append(('acknowledge', threading.get_ident()))

        def confirm(self, confirmation: JoinConfirm) -> Attached:
            del confirmation
            operations.append(('confirm', threading.get_ident()))
            return Attached(identity)

        def event_channel_lost(self, group: str, *, reason: str) -> None:
            del group
            assert reason == 'event-channel-lost'
            operations.append(('lost', threading.get_ident()))
            channel_lost.set()

        def finalize(self, finalization: JoinFinalize) -> JoinFinalized:
            del finalization
            operations.append(('finalize', threading.get_ident()))
            return JoinFinalized(identity)

        def request(
            self,
            request: JoinRequest,
            channel: GroupChannelInterface,
        ) -> JoinTarget:
            del request
            operations.append(('request', threading.get_ident()))
            assert channel.offer(
                EventDelivery(EncodedEvent.from_event_frame(21, encode_event(_event(21)))),
            )
            return JoinTarget(identity)

    source = _Source()
    stop = threading.Event()

    def run_source() -> None:
        while not stop.is_set():
            server.poll(0.001)
            server.dispatch_join_requests(source)

    with server:
        assert server.install_grant(grant) == ipc_module.EventPeerReady(
            source='events',
            group='group-a',
            dependent_spawn_instance='dependent-1',
        )
        source_thread = threading.Thread(target=run_source)
        source_thread.start()
        try:
            client.connect()
            channel = GroupChannel(
                group='group-a',
                ordering='strict',
                max_events=8,
                max_bytes=_QUEUE_BYTES,
            )
            attempt = JoinAttemptIdentity(
                site='main',
                source='events',
                group='group-a',
                join_kind='strict',
                group_generation=1,
                attempt_token=_ATTEMPT_ID,
            )

            assert client.request(JoinRequest(attempt), channel) == JoinTarget(identity)
            assert client.confirm(JoinConfirm(identity)) == Attached(identity)
            assert client.finalize(JoinFinalize(identity)) == JoinFinalized(identity)
            client.acknowledge_strict_control(
                StrictCapacityControl(identity=identity, abort_limit=4)
            )
            client.abort(JoinAbort(identity))
            _wait_for_channel_count(client, channel, 1)
            (delivery,) = channel.drain()
            assert isinstance(delivery, EventDelivery)
            assert delivery.encoded.event_id == 21
            client.close()
            assert channel_lost.wait(1.0)
        finally:
            stop.set()
            source_thread.join(1.0)

    assert tuple(name for name, _thread_id in operations) == (
        'request',
        'confirm',
        'finalize',
        'acknowledge',
        'abort',
        'lost',
    )
    assert {thread_id for _name, thread_id in operations} == {source_thread.ident}


def test_event_server_creates_socket_privately_before_bind_returns(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    real_socket = socket.socket
    observed_modes: list[int] = []

    class ObservedSocket(real_socket):
        def bind(self, address: object) -> None:
            path = cast('str | bytes', address)
            super().bind(path)
            observed_modes.append(stat.S_IMODE(os.lstat(path).st_mode))

    monkeypatch.setattr(ipc_module.socket, 'socket', ObservedSocket)
    original_umask = os.umask(0)
    try:
        with EventServer(
            path=path,
            source='events',
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=2,
            queue_max_bytes=_QUEUE_BYTES,
        ):
            pass
    finally:
        os.umask(original_umask)

    assert observed_modes == [0o600]


def test_event_server_enforces_frame_and_queue_byte_bounds(tmp_path: Path) -> None:
    path = _socket_path(tmp_path)
    oversized = EventDelivery(
        EncodedEvent.from_event_frame(50, encode_event(_event(50, text='x' * 1_000)))
    )
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=64,
        queue_max_entries=4,
        queue_max_bytes=_QUEUE_BYTES,
    )
    with pytest.raises(_FrameError, match='exceeds'):
        server._publish_connection('group-a', 'missing', oversized)

    event = EventDelivery(EncodedEvent.from_event_frame(51, encode_event(_event(51))))
    with EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=4,
        queue_max_bytes=event.byte_count - 1,
    ) as server:
        _install_grant(server, 'group-a')
        client = EventClient(
            path=path,
            source='events',
            group='group-a',
            spawn_instance='spawn-1',
            capability='capability-1',
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=4,
            queue_max_bytes=_QUEUE_BYTES,
            coordination_deadline_secs=30.0,
            stop=threading.Event(),
        )
        client.connect()
        for _ in range(100):
            server.poll(0.001)
            if server._connected_groups:
                break
        _channel, remote = _join_client(server, client, max_events=4)
        assert not remote.offer(event)

        assert server._group_status('group-a')._reason == 'queue-byte-exhausted'
        client.close()


def test_event_server_keeps_malformed_frames_distinct_from_eof(tmp_path: Path) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )
    read_peer = cast(
        'Callable[[EventServer, object], None]',
        vars(EventServer)['_read_peer'],
    )
    with server:
        _install_grant(server, 'group-a')
        client.connect()
        _wait_for_client_state(server, client, connected=True)
        peer = _server_peer(server, 'event')
        _script_peer_recv(peer, struct.pack('!I', 0))

        read_peer(server, peer)

        status = server._group_status('group-a')
        assert not status._connected
        assert status._reason == 'authentication-failed'
    client.close()


def test_event_server_paced_offer_keeps_a_full_strict_peer_attached(tmp_path: Path) -> None:
    path = _socket_path(tmp_path)
    first = EventDelivery(EncodedEvent.from_event_frame(1, encode_event(_event(1))))
    second = EventDelivery(EncodedEvent.from_event_frame(2, encode_event(_event(2))))
    with EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=1,
        queue_max_bytes=_QUEUE_BYTES,
    ) as server:
        _install_grant(server, 'strict')
        client = EventClient(
            path=path,
            source='events',
            group='strict',
            spawn_instance='spawn-1',
            capability='capability-1',
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=2,
            queue_max_bytes=_QUEUE_BYTES,
            coordination_deadline_secs=30.0,
            stop=threading.Event(),
        )
        client.connect()
        for _ in range(100):
            server.poll(0.001)
            if server._connected_groups:
                break
        channel, remote = _join_client(
            server, client, group='strict', ordering='strict', max_events=2
        )

        assert remote.offer_paced(first)
        assert not remote.offer_paced(second)
        status = server._group_status('strict')
        assert status._connected
        assert status.queued_count == 1

        _pump(server, (client,), (channel,))
        assert channel.drain() == (first,)
        assert remote.offer_paced(second)
        client.close()


def test_event_server_propagates_non_disconnect_receive_errors(tmp_path: Path) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )
    read_peer = cast(
        'Callable[[EventServer, object], None]',
        vars(EventServer)['_read_peer'],
    )
    with server:
        _install_grant(server, 'group-a')
        client.connect()
        _wait_for_client_state(server, client, connected=True)
        peer = _server_peer(server, 'event')
        _script_peer_recv(peer, OSError(errno.EIO, 'input/output error'))

        with pytest.raises(OSError, match='input/output error'):
            read_peer(server, peer)

        assert server._group_status('group-a')._connected
        assert server._connected_groups == ('group-a',)
    client.close()


def test_event_server_rechecks_parent_security_before_bind() -> None:
    with tempfile.TemporaryDirectory(prefix='sged-', dir='/tmp') as root:
        parent = Path(root) / 'unsafe'
        parent.mkdir(mode=0o777)
        parent.chmod(0o777)
        path = parent / 'events.sock'
        server = EventServer(
            path=path,
            source='events',
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=2,
            queue_max_bytes=_QUEUE_BYTES,
        )

        with pytest.raises(SecurityError, match='parent directory is writable'):
            server.start()

        assert not path.exists()


def test_event_server_rejects_post_authentication_input_from_event_role(tmp_path: Path) -> None:
    path = _socket_path(tmp_path)
    call = vars(ipc_module)['_JoinCall'](
        _request_id=1,
        _value=GroupBootstrapRequest(
            source='events',
            group='group-a',
            group_generation=1,
            start_mode='resume',
        ),
    )
    encode_join_call = cast(
        'Callable[[object], _OutboundFrame]',
        vars(ipc_module)['_encode_join_call'],
    )
    request_frame = _frame_bytes(encode_join_call(call))
    with (
        socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client,
        EventServer(
            path=path,
            source='events',
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=2,
            queue_max_bytes=_QUEUE_BYTES,
        ) as server,
    ):
        _install_grant(server, 'group-a')
        client.connect(os.fspath(path))
        client.settimeout(0.0)
        server.poll(0.001)
        peer = _server_peer(server, None)
        _write_event_server_stream(server, client, _event_authentication_frame('event'))
        _service_event_server_until(
            server,
            lambda: vars(peer)['_role'] == 'event',
        )
        assert vars(peer)['_role'] == 'event'

        _write_event_server_stream(server, client, request_frame)
        _service_event_server_until(
            server,
            lambda: server._group_status('group-a')._reason == 'authentication-failed',
        )

        status = server._group_status('group-a')
        assert not status._connected
        assert status._reason == 'authentication-failed'
        assert peer._socket.fileno() == -1


def test_event_server_replaces_only_safe_stale_socket(tmp_path: Path) -> None:
    path = _socket_path(tmp_path)
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(os.fspath(path))
    path.chmod(0o600)
    stale.close()

    with EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    ):
        assert stat.S_ISSOCK(path.stat().st_mode)
    unsafe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    unsafe.bind(os.fspath(path))
    path.chmod(0o666)
    unsafe.close()
    try:
        with pytest.raises(SecurityError, match='mode 0600'):
            EventServer(
                path=path,
                source='events',
                max_frame_bytes=_MAX_FRAME_BYTES,
                queue_max_entries=2,
                queue_max_bytes=_QUEUE_BYTES,
            ).start()
        assert path.exists()
    finally:
        path.unlink(missing_ok=True)


def test_event_server_restores_umask_after_interrupted_bind_and_restarts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    real_socket = socket.socket
    observed_modes: list[int] = []
    observed_umasks: list[int] = []

    def _current_umask() -> int:
        value = os.umask(0)
        os.umask(value)
        return value

    class InterruptedSocket(real_socket):
        def bind(self, address: object) -> None:
            path = cast('str | bytes', address)
            super().bind(path)
            observed_modes.append(stat.S_IMODE(os.lstat(path).st_mode))
            observed_umasks.append(_current_umask())
            raise OSError('simulated post-bind interruption')

    monkeypatch.setattr(ipc_module.socket, 'socket', InterruptedSocket)
    original_umask = os.umask(0)
    try:
        server = EventServer(
            path=path,
            source='events',
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=2,
            queue_max_bytes=_QUEUE_BYTES,
        )
        with pytest.raises(OSError, match='simulated post-bind interruption'):
            server.start()

        assert observed_modes == [0o600]
        assert observed_umasks == [0o177]
        assert _current_umask() == 0

        monkeypatch.setattr(ipc_module.socket, 'socket', real_socket)
        with EventServer(
            path=path,
            source='events',
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=2,
            queue_max_bytes=_QUEUE_BYTES,
        ):
            assert path.is_socket()
    finally:
        os.umask(original_umask)
        path.unlink(missing_ok=True)


def test_fanout_bridges_shared_frames_to_independent_joined_peers(tmp_path: Path) -> None:
    fanout = EventFanout()
    framed: list[object] = []

    def frame(message: ipc_module.FanoutMessage) -> _OutboundFrame:
        framed.append(message)
        return _encode_fanout_message(message)

    path = _socket_path(tmp_path)
    with EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=1,
        queue_max_bytes=_QUEUE_BYTES,
        frame_encoder=frame,
    ) as server:
        clients = tuple(_event_client(path, group) for group in ('first', 'second', 'stalled'))
        sinks: list[GroupChannel] = []
        remotes: list[GroupChannelInterface] = []
        try:
            for client, group in zip(clients, ('first', 'second', 'stalled'), strict=True):
                _install_grant(server, group)
                client.connect()
                sink, remote = _join_client(server, client, group=group)
                sinks.append(sink)
                remotes.append(remote)
                fanout.attach(remote)
            encoded = EncodedEvent.from_event_frame(8, encode_event(_event(8)))
            assert remotes[2].offer(EventDelivery(encoded))
            framed.clear()
            statuses = fanout.publish_events((encoded,))
            assert tuple(status.group for status in statuses) == ('first', 'second', 'stalled')
            by_group = {status.group: status for status in statuses}
            assert framed[0] is framed[1] is framed[2]
            assert isinstance(framed[0], EventDelivery)
            assert framed[0].encoded is encoded
            assert not by_group['stalled'].attached
            assert by_group['stalled'].reason == 'connection-replaced'
            assert server._group_status('stalled')._reason == 'queue-count-exhausted'
            assert by_group['first'].attached
            assert by_group['second'].attached
            _pump(server, clients[:2], tuple(sinks[:2]))
            assert sinks[0].drain() == sinks[1].drain() == (EventDelivery(encoded),)
        finally:
            for client in clients:
                client.close()


def test_float_wire_decoders_preserve_nullable_join_target_clock() -> None:
    decode_join_target = cast(
        'Callable[[Mapping[str, object]], JoinTarget]',
        vars(ipc_module)['_decode_join_target'],
    )

    decoded = decode_join_target(
        {
            'identity': {
                'attempt_token': _ATTEMPT_ID,
                'boundary': 20,
                'group': 'group-a',
                'group_generation': 7,
                'join_kind': 'strict',
                'site': 'main',
                'source': 'events',
                'source_generation': 'source-1',
            },
            'source_successful_clock': None,
            'stream_incarnation': 'incarnation-1',
        }
    )

    assert decoded.source_successful_clock is None


def test_frame_reader_rejects_malformed_and_oversized_lengths() -> None:
    reader = _frame_reader(max_frame_bytes=32)

    with pytest.raises(_FrameError, match='exceeds'):
        reader._feed((33).to_bytes(4, 'big'))
    with pytest.raises(_FrameError, match='positive'):
        _frame_reader(max_frame_bytes=32)._feed(b'\x00\x00\x00\x00')


def test_group_bootstrap_observation_runs_on_source_thread(
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=8,
        queue_max_bytes=_QUEUE_BYTES,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='dependent-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=8,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )
    request_value = GroupBootstrapRequest(
        source='events',
        group='group-a',
        group_generation=7,
        start_mode='resume',
    )
    observation = GroupBootstrapObservation(
        source='events',
        group='group-a',
        group_generation=7,
        source_generation='source-1',
        stream_incarnation='incarnation-1',
        captured_frontier=41,
        source_successful_clock=12.5,
    )
    operations: list[tuple[str, int]] = []

    class _Source(_JoinSourceStub):
        def event_channel_lost(self, group: str, *, reason: str) -> None:
            del group
            assert reason == 'event-channel-lost'

        def observe_group(
            self,
            request: GroupBootstrapRequest,
        ) -> GroupBootstrapObservation:
            assert request == request_value
            operations.append(('observe', threading.get_ident()))
            return observation

    source = _Source()
    stop = threading.Event()

    def run_source() -> None:
        while not stop.is_set():
            server.poll(0.001)
            server.dispatch_join_requests(source)

    with server:
        _install_grant(
            server,
            'group-a',
            spawn_instance='dependent-1',
        )
        source_thread = threading.Thread(target=run_source)
        source_thread.start()
        try:
            client.connect()
            assert client.observe_group(request_value) == observation
            assert client.observe_group(request_value) == observation
        finally:
            client.close()
            stop.set()
            source_thread.join(1.0)

    assert tuple(name for name, _thread_id in operations) == ('observe', 'observe')
    assert {thread_id for _name, thread_id in operations} == {source_thread.ident}


def test_group_bootstrap_pending_codec_is_closed_exact_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    decode_join_reply = cast(
        'Callable[[Mapping[str, object]], object]',
        vars(ipc_module)['_decode_join_reply'],
    )
    decode_event_wire = cast(
        'Callable[[bytes], object]',
        vars(ipc_module)['_decode_event_wire'],
    )
    encode_join_reply = cast(
        'Callable[[object], _OutboundFrame]',
        vars(ipc_module)['_encode_join_reply'],
    )
    pending = GroupBootstrapPending(
        source='events',
        group='group-a',
        group_generation=7,
        retry_after_secs=60.0,
    )
    encoded = encode_join_reply(
        SimpleNamespace(
            _request_id=1,
            _result=pending,
            _error=None,
        )
    )

    decoded = decode_event_wire(_frame_bytes(encoded))

    assert vars(decoded)['_result'] == pending
    assert {field.name for field in fields(pending)} == {
        'group',
        'group_generation',
        'retry_after_secs',
        'source',
    }

    payload: dict[str, object] = {
        'group': 'group-a',
        'group_generation': 7,
        'retry_after_secs': 1.0,
        'source': 'events',
    }

    def _decode_pending(value: Mapping[str, object]) -> object:
        return decode_join_reply(
            {
                'error': None,
                'request_id': 1,
                'result': dict(value),
                'result_kind': 'bootstrap-pending',
            }
        )

    assert vars(_decode_pending(payload))['_result'] == GroupBootstrapPending(
        source='events',
        group='group-a',
        group_generation=7,
        retry_after_secs=1.0,
    )
    with pytest.raises(_FrameError, match='fields'):
        _decode_pending({**payload, 'extra': True})
    with pytest.raises(_FrameError, match='fields'):
        _decode_pending(
            {key: value for key, value in payload.items() if key != 'retry_after_secs'}
        )
    for field_name, invalid_value in (
        ('source', ''),
        ('group', ''),
        ('group_generation', True),
        ('group_generation', -1),
    ):
        invalid_identity = {**payload, field_name: invalid_value}
        with pytest.raises(_FrameError, match=r'identity|generation'):
            _decode_pending(invalid_identity)
    for invalid_delay in (
        True,
        0.0,
        -0.1,
        60.000_001,
        float('inf'),
        float('nan'),
        '1.0',
    ):
        with pytest.raises(_FrameError, match='delay'):
            _decode_pending({**payload, 'retry_after_secs': invalid_delay})

    request = GroupBootstrapRequest(
        source='events',
        group='group-a',
        group_generation=7,
        start_mode='initialize',
    )
    client = EventClient(
        path=tmp_path / 'unused.sock',
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=1,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )
    for conflicting in (
        GroupBootstrapPending(
            source='other',
            group='group-a',
            group_generation=7,
            retry_after_secs=1.0,
        ),
        GroupBootstrapPending(
            source='events',
            group='other',
            group_generation=7,
            retry_after_secs=1.0,
        ),
        GroupBootstrapPending(
            source='events',
            group='group-a',
            group_generation=8,
            retry_after_secs=1.0,
        ),
    ):

        def return_conflicting(
            _value: object,
            *,
            result: GroupBootstrapPending = conflicting,
        ) -> GroupBootstrapPending:
            return result

        monkeypatch.setattr(
            client,
            '_rpc',
            return_conflicting,
        )
        with pytest.raises(JoinProtocolError, match='identity'):
            client.observe_group(request)


def test_health_frame_preserves_false_liveness_and_typed_collections() -> None:
    update = _complete_health_update()
    update = replace(
        update,
        worker=replace(
            update.worker, monitor_alive=False, source_outages=(('events', 'unavailable'),)
        ),
        sources=(replace(update.sources[0], strict_retention=(('editorial', 1, 2, 3),)),),
        groups=(
            replace(
                update.groups[0],
                outstanding_late_ids=(21, 22),
                outstanding_late_id_count=3,
                outstanding_late_ids_truncated=True,
                conditions=('catching-up',),
            ),
        ),
    )
    decoded = _decode_control_message(encode_control_message(update))
    assert decoded == update
    assert isinstance(decoded, HealthUpdate)
    assert decoded.worker.monitor_alive is False
    snapshot = aggregate_status(
        workers=(decoded.worker,),
        sources=decoded.sources,
        groups=decoded.groups,
        now=10.0,
        stale_after_secs=5.0,
    )
    assert snapshot.liveness is False


def test_health_update_has_one_closed_typed_component_schema() -> None:
    assert {field.name for field in fields(HealthUpdate)} == {
        'event_sockets',
        'groups',
        'sources',
        'watchdogs',
        'worker',
    }


def test_health_update_rejects_closed_watchdog_schema_violations() -> None:
    watchdog = WatchdogHealth(
        kind='operator',
        site='main',
        source='events',
        group=None,
        worker_instance='spawn-1',
        phase='internal',
        heartbeat_at=10.0,
        deadline_at=20.0,
        operator_command='park',
    )
    update = replace(_health_update(), watchdogs=(watchdog,))
    encoded = encode_control_message(update)

    duplicate_body = encoded[4:].replace(
        b'"phase":"internal"',
        b'"phase":"internal","phase":"idle"',
        1,
    )
    with pytest.raises(_FrameError, match='duplicate'):
        _decode_control_message(struct.pack('!I', len(duplicate_body)) + duplicate_body)

    envelope = cast('dict[str, object]', json.loads(encoded[4:]))
    payload = cast('dict[str, object]', envelope['payload'])
    encoded_watchdogs = cast('list[dict[str, object]]', payload['watchdogs'])
    encoded_watchdogs[0]['registration_token'] = os.urandom(16).hex()
    unknown_body = json.dumps(envelope, separators=(',', ':')).encode()
    with pytest.raises(_FrameError, match=r'watchdog health.*fields'):
        _decode_control_message(struct.pack('!I', len(unknown_body)) + unknown_body)

    del encoded_watchdogs[0]['registration_token']
    encoded_watchdogs[0]['operator_command'] = 'token=secret-material'
    secret_body = json.dumps(envelope, separators=(',', ':')).encode()
    with pytest.raises(_FrameError, match='secret-looking text'):
        _decode_control_message(struct.pack('!I', len(secret_body)) + secret_body)

    with pytest.raises(ValueError, match='secret-looking text'):
        replace(
            update,
            watchdogs=(replace(watchdog, operator_command='token=secret-material'),),
        )


def test_health_update_round_trips_bounded_late_id_metadata() -> None:
    late_ids = tuple(range(100))
    group = GroupHealth(
        site='main',
        source='events',
        group='editorial',
        worker_instance='spawn-1',
        execution='sync',
        ordering='strict',
        phase='idle',
        status='running',
        cursor=99,
        current_event_id=None,
        current_plugin_id=None,
        current_callback_id=None,
        delivery_attempt=None,
        heartbeat_at=10.0,
        heartbeat_age=0.0,
        queue_event_count=0,
        queue_byte_count=0,
        catchup_frontier=None,
        pending_catchup_epoch_count=0,
        oldest_catchup_epoch_age=None,
        join_phase='attached',
        outstanding_late_obligation_count=1,
        outstanding_late_id_count=50_000,
        outstanding_late_ids=late_ids,
        outstanding_late_ids_truncated=True,
        parked=False,
        park_reason=None,
        failure_kind=None,
        failure_text=None,
        checkpoint_state='complete',
        conditions=(),
    )
    update = replace(_health_update(), groups=(group,))

    assert _decode_control_message(encode_control_message(update)) == update


def test_health_update_round_trips_exact_watchdog_inventory() -> None:
    update = _complete_health_update()
    encoded = encode_control_message(update)
    envelope = cast('dict[str, object]', json.loads(encoded[4:]))
    payload = cast('dict[str, object]', envelope['payload'])
    encoded_watchdogs = cast('list[dict[str, object]]', payload['watchdogs'])

    assert tuple(payload) == ('event_sockets', 'groups', 'sources', 'watchdogs', 'worker')
    assert set(encoded_watchdogs[0]) == {
        'callback_id',
        'deadline_at',
        'delivery_attempt',
        'event_id',
        'group',
        'heartbeat_at',
        'kind',
        'operator_command',
        'phase',
        'plugin_id',
        'site',
        'source',
        'worker_instance',
    }
    assert 'registration_token' not in encoded_watchdogs[0]
    assert 'transition_sequence' not in encoded_watchdogs[0]
    assert _decode_control_message(encoded) == update


def test_health_update_round_trips_write_watch_phases() -> None:
    update = _complete_health_update()
    update = replace(
        update,
        watchdogs=(
            replace(update.watchdogs[0], phase='write-hold'),
            replace(update.watchdogs[1], phase='write-lock-wait'),
            update.watchdogs[2],
        ),
        sources=(replace(update.sources[0], phase='write-hold'),),
        groups=(replace(update.groups[0], phase='write-lock-wait'),),
    )

    assert _decode_control_message(encode_control_message(update)) == update


def test_join_call_round_trips_capacity_control() -> None:
    identity = LiveJoinIdentity(
        site='main',
        source='events',
        group='group-a',
        join_kind='strict',
        source_generation='source-1',
        group_generation=3,
        boundary=20,
        attempt_token=_ATTEMPT_ID,
    )
    control = StrictCapacityControl(identity=identity, abort_limit=7)
    call = ipc_module._JoinCall(29, control)

    decoded = ipc_module._decode_join_call(_frame_bytes(ipc_module._encode_join_call(call))[4:])

    assert decoded._request_id == 29
    assert isinstance(decoded._value, StrictCapacityControl)
    assert decoded._value.identity == identity
    assert decoded._value.abort_limit == 7


def test_join_call_round_trips_ordering_control() -> None:
    identity = LiveJoinIdentity(
        site='main',
        source='events',
        group='group-a',
        join_kind='strict',
        source_generation='source-1',
        group_generation=3,
        boundary=20,
        attempt_token=_ATTEMPT_ID,
    )
    control = StrictOrderingControl(
        identity=identity,
        event_id=22,
        last_delivered_id=21,
        delivery_floor=20,
        action='skip',
    )
    call = ipc_module._JoinCall(31, control)

    decoded = ipc_module._decode_join_call(_frame_bytes(ipc_module._encode_join_call(call))[4:])

    assert decoded._request_id == 31
    assert isinstance(decoded._value, StrictOrderingControl)
    assert decoded._value.identity == identity
    assert decoded._value.event_id == 22
    assert decoded._value.last_delivered_id == 21
    assert decoded._value.delivery_floor == 20
    assert decoded._value.action == 'skip'


def test_join_rpc_disconnect_while_event_queue_is_full_raises_connection_error(
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=1,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )
    identity = LiveJoinIdentity(
        site='main',
        source='events',
        group='group-a',
        join_kind='eager',
        source_generation='source-1',
        group_generation=1,
        boundary=60,
        attempt_token=_ATTEMPT_ID,
    )

    remotes: list[GroupChannelInterface] = []

    class _Source(_JoinSourceStub):
        def confirm(self, confirmation: JoinConfirm) -> Attached:
            del confirmation
            remotes[0].detach('source-disconnected')
            return Attached(identity)

        def event_channel_lost(self, group: str, *, reason: str) -> None:
            del group
            assert reason == 'event-channel-lost'

        def request(
            self,
            request: JoinRequest,
            channel: GroupChannelInterface,
        ) -> JoinTarget:
            del request
            remotes.append(channel)
            return JoinTarget(identity)

    source_thread = _SourceThreadHarness(server, _Source())
    with server:
        _install_grant(server, 'group-a')
        source_thread._start()
        try:
            client.connect()
            channel = GroupChannel(
                group='group-a',
                ordering='eager',
                max_events=1,
                max_bytes=_QUEUE_BYTES,
            )
            attempt = JoinAttemptIdentity(
                site='main',
                source='events',
                group='group-a',
                join_kind='eager',
                group_generation=1,
                attempt_token=_ATTEMPT_ID,
            )
            assert client.request(JoinRequest(attempt), channel) == JoinTarget(identity)

            def _publish_full_event() -> None:
                assert remotes[0].offer(
                    EventDelivery(EncodedEvent.from_event_frame(61, encode_event(_event(61)))),
                )

            source_thread._call(_publish_full_event)
            _wait_for_channel_count(client, channel, 1)
            assert channel.status.queued_count == 1
            result = _run_bounded_client_call(
                client,
                lambda: client.confirm(JoinConfirm(identity)),
            )
            assert isinstance(result, ConnectionError)
            assert not client.connected
            assert channel.status.queued_count == 0
            assert not channel.status.attached
        finally:
            client.close()
            source_thread._close()


def test_join_target_decoder_rejects_negative_successful_clock() -> None:
    decode_join_target = cast(
        'Callable[[Mapping[str, object]], JoinTarget]',
        vars(ipc_module)['_decode_join_target'],
    )

    with pytest.raises(
        _FrameError,
        match='join target successful clock must be numeric or null',
    ):
        decode_join_target(
            {
                'identity': {
                    'attempt_token': _ATTEMPT_ID,
                    'boundary': 20,
                    'group': 'group-a',
                    'group_generation': 7,
                    'join_kind': 'strict',
                    'site': 'main',
                    'source': 'events',
                    'source_generation': 'source-1',
                },
                'source_successful_clock': -0.1,
                'stream_incarnation': 'incarnation-1',
            }
        )


def test_json_encoder_materializes_only_the_top_level_mapping() -> None:
    encode_json = cast(
        'Callable[[Mapping[str, object]], bytes]',
        vars(ipc_module)['_encode_json'],
    )
    payload: dict[str, object] = {'kind': 'health', 'version': _CONTROL_PROTOCOL_VERSION}

    assert encode_json(MappingProxyType(payload)) == encode_json(payload)

    with pytest.raises(_FrameError):
        encode_json({'nested': MappingProxyType({'value': 1})})
    with pytest.raises(_FrameError):
        encode_json({'value': float('nan')})


def test_operator_command_and_result_round_trip_with_request_correlation() -> None:
    command = OperatorCommand(
        request_id='operator-request-2',
        command='replay',
        arguments={
            'site': 'main',
            'group': 'editorial',
            'from_event_id': 123,
            'acknowledge_truncation': False,
            'reason': 'approved replay',
        },
        identity=AuditIdentity(local_uid=501, username='operator', pid=456),
    )
    result = CommandResult(
        command='replay',
        ok=True,
        message='accepted',
        request_id=command.request_id,
        result={'revision': 9, 'status': 'parked'},
    )

    assert _decode_control_message(encode_control_message(command)) == command
    assert _decode_control_message(encode_control_message(result)) == result


def test_partial_writer_finishes_frames_without_whole_frame_buffer_space() -> None:

    class _ShortSocket:
        def __init__(self) -> None:
            self._output = bytearray()
            self._calls = 0

        def send(self, value: memoryview) -> int:
            self._calls += 1
            accepted = min(3, len(value))
            self._output.extend(value[:accepted])
            return accepted

    message = EventDelivery(
        EncodedEvent.from_event_frame(2, encode_event(_event(2, text='x' * 100)))
    )
    frame = _encode_fanout_message(message)
    sock = _ShortSocket()
    writer = _partial_writer()
    writer._append(frame)

    while writer._pending_count:
        writer._flush(sock)

    assert bytes(sock._output) == _frame_bytes(frame)
    assert sock._calls > 2


def test_private_event_endpoint_authenticates_and_reports_socket_buffers(
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=4,
        queue_max_bytes=_QUEUE_BYTES,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=4,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )
    with server:
        _install_grant(server, 'group-a')
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        client.connect()
        assert client.connected
        assert server._connected_groups == ()
        for _ in range(100):
            server.poll(0.001)
            if server._connected_groups == ('group-a',):
                break
        channel, remote = _join_client(server, client, max_events=4)
        assert server._connected_groups == ('group-a',)
        remote.offer(EventDelivery(EncodedEvent.from_event_frame(3, encode_event(_event(3)))))
        _pump(server, (client,), (channel,))
        (received,) = channel.drain()
        assert isinstance(received, EventDelivery)
        assert decode_event(received.encoded.event_frame) == _event(3)
        assert server.socket_buffers.requested == _MAX_FRAME_BYTES
        assert client.socket_buffers.requested == _MAX_FRAME_BYTES
        assert server.socket_buffers.send > 0
        assert server.socket_buffers.receive > 0
        assert client.socket_buffers.send > 0
        assert client.socket_buffers.receive > 0
    client.close()
    assert not path.exists()


def test_protocol_versions_are_fixed() -> None:
    assert _CONTROL_PROTOCOL_VERSION == 1
    assert _EVENT_AUTH_PROTOCOL_VERSION == 1


def test_range_changed_golden_body_free_wire() -> None:
    message = fanout_module.RangeChanged()
    frame = _encode_fanout_message(message)
    assert _frame_bytes(frame) == (
        b'\x00\x00\x00_\x00\x00\x00['
        b'{"kind":"range-changed","metadata":{},"schema":'
        b'"sg-event-daemon-event-channel","version":1}'
    )
    assert _decode_event_wire(_frame_bytes(frame)) == message
    assert message.byte_count == len(_frame_bytes(frame))


def test_raw_frame_reader_read_size_fences_authentication_from_next_frame() -> None:
    factory = ipc_module._RawFrameReader
    reader = factory(max_frame_bytes=_MAX_FRAME_BYTES)
    authentication = b'authentication'
    request = b'normal request'
    stream = (
        struct.pack('!I', len(authentication))
        + authentication
        + struct.pack('!I', len(request))
        + request
    )

    offset = 0
    decoded: list[object] = []
    while offset < len(stream):
        chunk = stream[offset : offset + reader._read_size]
        offset += len(chunk)
        batch = reader._feed(chunk)
        assert len(batch) <= 1
        decoded.extend(batch)

    assert decoded == [authentication, request]


def test_remote_range_changed_is_dropped_when_the_writer_is_full(tmp_path: Path) -> None:
    path = _socket_path(tmp_path)
    with EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=1,
        queue_max_bytes=_QUEUE_BYTES,
    ) as server:
        _install_grant(server, 'group-a')
        client = _event_client(path)
        try:
            client.connect()
            sink, remote = _join_client(server, client)
            first = EventDelivery(EncodedEvent.from_event_frame(7, encode_event(_event(7))))
            assert remote.offer(first)
            assert not remote.offer_hint(fanout_module.RangeChanged())
            assert server._group_status('group-a')._connected
            assert server._group_status('group-a').queued_count == 1
            _pump(server, (client,), (sink,))
            assert sink.drain() == (first,)
            assert remote.offer_hint(fanout_module.RangeChanged())
            assert remote.offer_hint(fanout_module.RangeChanged())
            assert server._group_status('group-a').queued_count == 0
            received: tuple[object, ...] = ()
            for _ in range(100):
                server.poll(0.001)
                client.poll(0.001)
                received = sink.drain()
                if received:
                    break
            assert received == (fanout_module.RangeChanged(),)
            second = EventDelivery(EncodedEvent.from_event_frame(8, encode_event(_event(8))))
            assert remote.offer_hint(fanout_module.RangeChanged())
            assert remote.offer(second)
            assert server._group_status('group-a').queued_count == 1
            _pump(server, (client,), (sink,))
            assert sink.drain() == (second, fanout_module.RangeChanged())
        finally:
            client.close()


def test_server_close_preserves_additional_unlink_failure_note(tmp_path: Path) -> None:
    first = OSError('selector close failed')
    later = RuntimeError('unlink failed')
    server = EventServer(
        path=_socket_path(tmp_path),
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    )
    vars(server)['_selector'] = SimpleNamespace(close=Mock(side_effect=first))
    vars(server)['_unlink_owned_socket'] = Mock(side_effect=later)

    with pytest.raises(OSError, match='selector close failed') as caught:
        server.close()

    assert caught.value is first
    assert first.__notes__ == ["additional cleanup failure: RuntimeError('unlink failed')"]
    assert vars(server)['_bound_identity'] is None


def test_source_health_codec_round_trips_incarnation_and_rejects_omission() -> None:
    source = SourceHealth(
        site='main',
        source='events',
        source_generation='source-generation-1',
        status='running',
        frontier=100,
        successful_clock=10.0,
        retry_attempt=0,
        next_retry_secs=None,
        open_range_count=0,
        late_id_count=0,
        last_error=None,
        stream_incarnation='incarnation-1',
    )
    update = replace(_health_update(), sources=(source,))
    encoded = encode_control_message(update)

    assert _decode_control_message(encoded) == update

    payload = cast('dict[str, object]', json.loads(encoded[4:]))
    health_payload = cast('dict[str, object]', payload['payload'])
    sources = cast('list[dict[str, object]]', health_payload['sources'])
    del sources[0]['stream_incarnation']
    body = json.dumps(payload, separators=(',', ':')).encode()
    malformed = struct.pack('!I', len(body)) + body

    with pytest.raises(_FrameError, match=r'source health.*fields'):
        _decode_control_message(malformed)


def test_stalled_event_group_does_not_block_peer_and_eof_drops_bodies(
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    with EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=1,
        queue_max_bytes=_QUEUE_BYTES,
    ) as server:
        _install_grant(server, 'stalled')
        _install_grant(server, 'live')
        stalled = EventClient(
            path=path,
            source='events',
            group='stalled',
            spawn_instance='spawn-1',
            capability='capability-1',
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=1,
            queue_max_bytes=_QUEUE_BYTES,
            coordination_deadline_secs=30.0,
            stop=threading.Event(),
        )
        live = EventClient(
            path=path,
            source='events',
            group='live',
            spawn_instance='spawn-1',
            capability='capability-1',
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=2,
            queue_max_bytes=_QUEUE_BYTES,
            coordination_deadline_secs=30.0,
            stop=threading.Event(),
        )
        stalled.connect()
        live.connect()
        for _ in range(100):
            server.poll(0.001)
            if server._connected_groups == ('live', 'stalled'):
                break
        stalled_channel, stalled_remote = _join_client(
            server, stalled, group='stalled', max_events=1
        )
        live_channel, live_remote = _join_client(server, live, group='live')
        remotes = (stalled_remote, live_remote)
        received_ids: list[int] = []

        def publish_until_live_receives(message: EventDelivery) -> None:
            for remote in remotes:
                remote.offer(message)
            _pump(server, (live,), (live_channel,))
            for received in live_channel.drain():
                assert isinstance(received, EventDelivery)
                received_ids.append(received.encoded.event_id)

        for event_id in range(1, 101):
            message = EventDelivery(
                EncodedEvent.from_event_frame(
                    event_id,
                    encode_event(_event(event_id, text='x' * 60_000)),
                )
            )
            publish_until_live_receives(message)
            assert server._group_status('live')._connected
            if server._group_status('stalled')._reason == 'queue-count-exhausted':
                break
        assert server._group_status('stalled')._reason == 'queue-count-exhausted'
        assert received_ids == sorted(received_ids)
        final = EventDelivery(EncodedEvent.from_event_frame(1_001, encode_event(_event(1_001))))
        publish_until_live_receives(final)
        assert 1_001 in received_ids
        live_remote.detach('source-eof')
        for _ in range(10):
            server.poll(0.01)
            live.poll(0.01)
            if not live.connected:
                break
        assert not live.connected
        assert live_channel.status.queued_count == 0
        assert live_channel.status.queued_bytes == 0
        assert not live_channel.status.attached
        assert live.detach_reason == 'source-eof'
        assert stalled_channel.status.queued_count == 0
        stalled.close()
        live.close()


def test_worker_assignment_round_trips_closed_group_and_source_assignment_unions() -> None:
    assignment = WorkerAssignment(
        worker_id='["site","main"]',
        spawn_instance='source-spawn-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(
            ipc_module.DeliveryGroupAssignment(
                source='events',
                group='active',
                group_generation=7,
                start_mode='resume',
            ),
        ),
        source_group_assignments=(
            ActiveSourceGroupAssignment(
                source='events',
                group='active',
                group_generation=7,
                start_mode='resume',
            ),
            PendingDispositionSourceGroupAssignment(
                source='events',
                group='pending',
                orphan_generation=8,
                reserved_generation=11,
            ),
        ),
        source_endpoints=(),
        event_grants=(),
    )

    assert _decode_control_message(encode_control_message(assignment)) == assignment
    assert tuple(item.name for item in fields(ActiveSourceGroupAssignment)) == (
        'source',
        'group',
        'group_generation',
        'start_mode',
    )
    assert tuple(item.name for item in fields(PendingDispositionSourceGroupAssignment)) == (
        'source',
        'group',
        'orphan_generation',
        'reserved_generation',
    )


def test_worker_assignment_round_trips_one_optional_recovery_directive_per_delivery_group() -> (
    None
):
    delivery_assignment_type = vars(ipc_module).get('DeliveryGroupAssignment')
    assert delivery_assignment_type is not None
    directive = ConditionalParkDirective(
        evidence_id='evidence-1',
        site='main',
        group='local',
        group_generation=7,
        failed_spawn_instance='failed-spawn-1',
        event_id=42,
        group_revision=11,
        observed_at_wall_clock=1234.5,
    )
    assignment = WorkerAssignment(
        worker_id='["site","main"]',
        spawn_instance='source-spawn-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(
            delivery_assignment_type(
                source='events',
                group='local',
                group_generation=7,
                start_mode='initialize',
                conditional_park=directive,
            ),
            delivery_assignment_type(
                source='events',
                group='optional',
                group_generation=9,
                start_mode='resume',
                conditional_park=None,
            ),
        ),
        source_group_assignments=(
            ActiveSourceGroupAssignment(
                source='events',
                group='local',
                group_generation=7,
                start_mode='initialize',
            ),
            ActiveSourceGroupAssignment(
                source='events',
                group='remote',
                group_generation=11,
                start_mode='resume',
            ),
        ),
        source_endpoints=(),
        event_grants=(),
    )

    assert _decode_control_message(encode_control_message(assignment)) == assignment
    assert tuple(item.name for item in fields(delivery_assignment_type)) == (
        'source',
        'group',
        'group_generation',
        'start_mode',
        'conditional_park',
    )


def test_worker_available_batch_freezes_at_call_and_preserves_fifo() -> None:
    sender, receiver = socket.socketpair()
    with (
        sender,
        closing(
            WorkerControl(receiver, max_frame_bytes=_MAX_FRAME_BYTES, send_deadline_secs=1)
        ) as control,
    ):
        first, second, later = (StopCommand(reason=name) for name in ('first', 'second', 'later'))
        sender.sendall(encode_control_message(first) + encode_control_message(second))
        assert control.receive(timeout=0) == first
        batch = control.receive_available()
        sender.sendall(encode_control_message(later))
        assert tuple(batch) == (second,)
        assert tuple(control.receive_available()) == (later,)
        assert tuple(control.receive_available()) == ()


@pytest.mark.parametrize('split', [2, 12])
def test_worker_available_batch_retains_partial_frame(split: int) -> None:
    sender, receiver = socket.socketpair()
    with (
        sender,
        closing(
            WorkerControl(receiver, max_frame_bytes=_MAX_FRAME_BYTES, send_deadline_secs=1)
        ) as control,
    ):
        message = StopCommand(reason='partial')
        frame = encode_control_message(message)
        sender.sendall(frame[:split])
        batch = control.receive_available()
        sender.sendall(frame[split:])
        assert tuple(batch) == ()
        assert tuple(control.receive_available()) == (message,)


@pytest.mark.parametrize('malformed', [False, True])
def test_worker_available_batch_preserves_message_before_terminal_error(malformed: bool) -> None:
    sender, receiver = socket.socketpair()
    with (
        sender,
        closing(
            WorkerControl(receiver, max_frame_bytes=_MAX_FRAME_BYTES, send_deadline_secs=1)
        ) as control,
    ):
        message = StopCommand(reason='last-valid')
        sender.sendall(encode_control_message(message))
        if malformed:
            sender.sendall(struct.pack('!I', _MAX_FRAME_BYTES + 1))
        else:
            sender.close()
        batch = control.receive_available()
        assert next(batch) == message
        with pytest.raises(ValueError if malformed else EOFError):
            next(batch)


@pytest.fixture(autouse=True)
def _private_short_socket_root() -> Iterator[None]:
    with tempfile.TemporaryDirectory(prefix='sed-', dir=Path(os.sep, 'tmp')) as directory:
        root = Path(directory)
        assert not _SHORT_SOCKET_ROOT
        _SHORT_SOCKET_ROOT.append(root)
        try:
            yield
        finally:
            assert _SHORT_SOCKET_ROOT.pop() == root


@pytest.mark.parametrize(
    ('frame', 'error'),
    [
        (b'\x00\x00\x00', 'truncated'),
        (struct.pack('!I', 0), 'positive'),
        (struct.pack('!I', 33), 'exceeds configured maximum'),
        (struct.pack('!I', 2) + b'{', 'size conflicts'),
        (struct.pack('!I', 1) + b'{}', 'size conflicts'),
        (struct.pack('!I', 1) + b'{', 'control frame is invalid'),
    ],
)
def test_control_message_decoder_rejects_invalid_frames(frame: bytes, error: str) -> None:
    with pytest.raises(_FrameError, match=error):
        _decode_control_message(frame, max_frame_bytes=32)


@pytest.mark.parametrize('role', ['event', 'coordination'])
@pytest.mark.parametrize(
    'expire_after_partial_send', [False, True], ids=['socket-timeout', 'expired']
)
def test_event_client_authentication_deadline_closes_both_private_sockets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    role: Literal['coordination', 'event'],
    expire_after_partial_send: bool,
) -> None:
    real_socket = socket.socket
    clock = [0.0]
    send_timeouts: list[float | None] = []
    send_frames: list[bytes] = []
    received = bytearray()

    def consume_connect(_timeout: float | None) -> None:
        clock[0] += 0.02

    def send_partial(value: memoryview, timeout: float | None) -> int:
        send_frames.append(bytes(value))
        send_timeouts.append(timeout)
        if len(send_frames) == 1:
            received.extend(value[:7])
            clock[0] = 0.1 if expire_after_partial_send else 0.06
            return 7
        assert timeout is not None
        clock[0] += timeout
        raise TimeoutError

    def monotonic() -> float:
        return clock[0]

    sockets = tuple(
        _ScriptedStartupSocket(
            real_socket(socket.AF_UNIX, socket.SOCK_STREAM),
            connect_action=consume_connect,
            send_action=send_partial if current_role == role else None,
        )
        for current_role in ('event', 'coordination')
    )
    scripted = deque(sockets)

    def socket_factory(_family: int, _kind: int) -> _ScriptedStartupSocket:
        return scripted.popleft()

    monkeypatch.setattr(ipc_module, 'time', SimpleNamespace(monotonic=monotonic))
    monkeypatch.setattr(ipc_module.socket, 'socket', socket_factory)
    client = EventClient(
        path=_socket_path(tmp_path),
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=0.1,
        stop=threading.Event(),
    )

    try:
        with pytest.raises(ConnectionError, match='event endpoint connection failed'):
            client.connect()

        frame = send_frames[0]
        assert 0 < len(received) < len(frame)
        assert received == frame[:7]
        first_remaining = 0.08 if role == 'event' else 0.06
        if expire_after_partial_send:
            assert send_frames == [frame]
            assert send_timeouts == [pytest.approx(first_remaining)]
        else:
            assert send_frames == [frame, frame[7:]]
            assert send_timeouts == [pytest.approx(first_remaining), pytest.approx(0.04)]
        assert clock[0] == pytest.approx(0.1)
        assert not scripted
        assert all(sock.fileno() == -1 for sock in sockets)
        state = vars(client)
        assert state['_socket'] is None
        assert state['_coordination_socket'] is None
        assert state['_selector'] is None
        assert state['_coordination_selector'] is None
        assert client.connected is False
    finally:
        for sock in sockets:
            sock.close()


@pytest.mark.parametrize(
    ('scheduling_delay', 'expected_remaining'),
    [(0.03, 0.01), (0.04, None)],
    ids=['remaining', 'expired'],
)
def test_event_client_later_connect_uses_remaining_startup_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scheduling_delay: float,
    expected_remaining: float | None,
) -> None:
    real_socket = socket.socket
    clock = [0.0]
    coordination_timeouts: list[float] = []

    def _consume_event_connect(timeout: float | None) -> None:
        assert timeout == pytest.approx(0.1)
        clock[0] += 0.06 + scheduling_delay

    def _stall_coordination_connect(timeout: float | None) -> None:
        assert timeout is not None
        coordination_timeouts.append(timeout)
        clock[0] += timeout
        raise TimeoutError

    def monotonic() -> float:
        return clock[0]

    sockets = (
        _ScriptedStartupSocket(
            real_socket(socket.AF_UNIX, socket.SOCK_STREAM),
            connect_action=_consume_event_connect,
        ),
        _ScriptedStartupSocket(
            real_socket(socket.AF_UNIX, socket.SOCK_STREAM),
            connect_action=_stall_coordination_connect,
        ),
    )
    scripted = deque(sockets)

    def socket_factory(_family: int, _kind: int) -> _ScriptedStartupSocket:
        return scripted.popleft()

    monkeypatch.setattr(ipc_module, 'time', SimpleNamespace(monotonic=monotonic))
    monkeypatch.setattr(ipc_module.socket, 'socket', socket_factory)
    client = EventClient(
        path=_socket_path(tmp_path),
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=0.1,
        stop=threading.Event(),
    )

    with pytest.raises(ConnectionError, match='event endpoint connection failed'):
        client.connect()

    if expected_remaining is None:
        assert coordination_timeouts == []
    else:
        assert coordination_timeouts == [pytest.approx(expected_remaining)]
    assert clock[0] == pytest.approx(0.1)
    assert all(sock.fileno() == -1 for sock in sockets)
    assert client.connected is False


@pytest.mark.parametrize(
    ('spawn_instance', 'capability', 'expected_uid'),
    [
        ('wrong-spawn', 'capability-1', os.geteuid()),
        ('spawn-1', 'wrong-capability', os.geteuid()),
        ('spawn-1', 'capability-1', os.geteuid() + 1),
    ],
)
def test_event_endpoint_rejects_mismatched_authentication(
    tmp_path: Path,
    spawn_instance: str,
    capability: str,
    expected_uid: int,
) -> None:
    path = _socket_path(tmp_path)
    with EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        expected_uid=expected_uid,
    ) as server:
        _install_grant(server, 'group-a')
        client = EventClient(
            path=path,
            source='events',
            group='group-a',
            spawn_instance=spawn_instance,
            capability=capability,
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=2,
            queue_max_bytes=_QUEUE_BYTES,
            coordination_deadline_secs=30.0,
            stop=threading.Event(),
        )
        client.connect()
        for _ in range(100):
            server.poll(0.001)
            client.poll(0.001)
            if not client.connected:
                break
        assert server._connected_groups == ()
        assert not client.connected
        client.close()


@pytest.mark.parametrize('source_status', ['range-query-page-cap', 'encoder-shutdown'])
def test_health_frame_round_trips_all_record_fields(source_status: str) -> None:
    update = _complete_health_update()
    update = replace(
        update,
        worker=replace(
            update.worker,
            monitor_alive=False,
            stuck_threads=('source:events',),
            breaker_open=True,
            breaker_reason='breaker opened',
            source_outages=(('events', 'provider unavailable'),),
            heartbeat_age=2.5,
            watchdog_failure='watchdog expired',
        ),
        watchdogs=(
            replace(
                update.watchdogs[1],
                event_id=101,
                plugin_id='plugin-b',
                callback_id='callback-b',
                delivery_attempt=3,
            ),
            update.watchdogs[2],
        ),
        sources=(
            replace(
                update.sources[0],
                status=cast('SourceRunStatus', source_status),
                retry_attempt=2,
                next_retry_secs=3.5,
                open_range_count=4,
                late_id_count=5,
                last_error='query stalled',
                worker_instance='spawn-1',
                initialized=True,
                pending_obligation_count=6,
                orphan_marker_count=7,
                strict_buffer_event_count=10,
                strict_buffer_byte_count=11,
                strict_retention=(('editorial', 12, 13, 14),),
                checkpoint_state='incomplete',
                stream_incarnation='stream-1',
                audit_count=15,
                database_size_bytes=16,
                phase='provider-query',
                heartbeat_age=1.5,
                overdue_reason='query overdue',
            ),
        ),
        groups=(
            replace(
                update.groups[0],
                current_event_id=101,
                current_plugin_id='plugin-b',
                current_callback_id='callback-b',
                delivery_attempt=3,
                heartbeat_age=2.5,
                queue_event_count=4,
                queue_byte_count=5,
                catchup_frontier=100,
                pending_catchup_epoch_count=6,
                oldest_catchup_epoch_age=7.5,
                outstanding_late_obligation_count=8,
                outstanding_late_id_count=2,
                outstanding_late_ids=(98, 99),
                parked=True,
                park_reason='operator parked',
                failure_kind='callback',
                failure_text='callback failed',
                conditions=('parked', 'callback-failed'),
                audit_count=9,
                overdue_reason='callback overdue',
            ),
        ),
        event_sockets=(replace(update.event_sockets[0], reason='peer unavailable'),),
    )
    decoded = _decode_control_message(encode_control_message(update))
    assert isinstance(decoded, HealthUpdate)
    for name in ('worker', 'watchdogs', 'sources', 'groups', 'event_sockets'):
        expected_records = getattr(update, name)
        actual_records = getattr(decoded, name)
        if name == 'worker':
            expected_records = (expected_records,)
            actual_records = (actual_records,)
        assert len(actual_records) == len(expected_records)
        for field in fields(expected_records[0]):
            if field.default is not MISSING:
                assert any(
                    getattr(record, field.name) != field.default for record in expected_records
                ), field.name
        for expected, actual in zip(expected_records, actual_records, strict=True):
            assert {field.name: getattr(actual, field.name) for field in fields(actual)} == {
                field.name: getattr(expected, field.name) for field in fields(expected)
            }


@pytest.mark.parametrize('started', [False, True])
def test_join_client_releases_thread_when_start_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    started: bool,
) -> None:
    original_start = _SourceThreadHarness._start
    harnesses: list[_SourceThreadHarness] = []
    initiating: list[AssertionError] = []

    def fail_start(harness: _SourceThreadHarness) -> None:
        harnesses.append(harness)
        if not started:
            error = AssertionError('thread creation failed')
            initiating.append(error)
            raise error
        ready = harness._started
        monkeypatch.setattr(ready, 'wait', Mock(return_value=False))
        try:
            original_start(harness)
        except AssertionError as error:
            initiating.append(error)
            raise

    monkeypatch.setattr(_SourceThreadHarness, '_start', fail_start)
    path = _socket_path(tmp_path)
    with EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    ) as server:
        client = _event_client(path)
        # Keep test failure bounded even when checking the unfixed helper.
        try:
            with pytest.raises(AssertionError) as caught:
                _join_client(server, client)
            assert caught.value is initiating[0]
            thread = cast('threading.Thread', vars(harnesses[0])['_thread'])
            assert not thread.is_alive()
        finally:
            if harnesses:
                thread = cast('threading.Thread', vars(harnesses[0])['_thread'])
                if thread.is_alive():
                    harnesses[0]._close()
            client.close()


@pytest.mark.parametrize('join_kind', ['eager', 'strict'])
def test_live_join_identity_ipc_round_trips_maximum_configured_values(
    join_kind: Literal['eager', 'strict'],
) -> None:
    maximum = 2**63 - 1
    identity = LiveJoinIdentity(
        site='s' * 64,
        source='o' * 64,
        group='g' * 64,
        join_kind=join_kind,
        source_generation='f' * 32,
        group_generation=maximum,
        boundary=maximum,
        attempt_token='a' * 32,
    )
    encode_join_reply = cast(
        'Callable[[object], _OutboundFrame]',
        vars(ipc_module)['_encode_join_reply'],
    )
    decode_event_wire = cast(
        'Callable[[bytes], object]',
        vars(ipc_module)['_decode_event_wire'],
    )

    decoded = decode_event_wire(
        _frame_bytes(
            encode_join_reply(
                SimpleNamespace(
                    _request_id=maximum,
                    _result=Attached(identity),
                    _error=None,
                )
            )
        )
    )

    assert vars(decoded)['_result'] == Attached(identity)


@pytest.mark.parametrize(
    'operation',
    ['abort', 'acknowledge', 'confirm', 'finalize'],
)
def test_event_join_rpc_rejects_another_group_identity(
    operation: str,
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=8,
        queue_max_bytes=_QUEUE_BYTES,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='dependent-1',
        capability='capability-a',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=8,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )
    identity = LiveJoinIdentity(
        site='main',
        source='events',
        group='group-b',
        join_kind='strict',
        source_generation='source-1',
        group_generation=1,
        boundary=20,
        attempt_token=_ATTEMPT_ID,
    )
    dispatched: list[str] = []

    class _Source(_JoinSourceStub):
        def abort(self, abort: JoinAbort) -> None:
            del abort
            dispatched.append('abort')

        def acknowledge_strict_control(
            self,
            control: StrictCapacityControl | StrictOrderingControl,
        ) -> None:
            del control
            dispatched.append('acknowledge')

        def confirm(self, confirmation: JoinConfirm) -> Attached:
            del confirmation
            dispatched.append('confirm')
            return Attached(identity)

        def event_channel_lost(self, group: str, *, reason: str) -> None:
            del group
            assert reason == 'event-channel-lost'

        def finalize(self, finalization: JoinFinalize) -> JoinFinalized:
            del finalization
            dispatched.append('finalize')
            return JoinFinalized(identity)

    source = _Source()
    stop = threading.Event()

    def run_source() -> None:
        while not stop.is_set():
            server.poll(0.001)
            server.dispatch_join_requests(source)

    with server:
        server.install_grant(
            ipc_module.EventPeerGrant(
                source='events',
                group='group-a',
                dependent_spawn_instance='dependent-1',
                capability='capability-a',
            )
        )
        server.install_grant(
            ipc_module.EventPeerGrant(
                source='events',
                group='group-b',
                dependent_spawn_instance='dependent-1',
                capability='capability-b',
            )
        )
        source_thread = threading.Thread(target=run_source)
        source_thread.start()
        try:
            client.connect()
            with pytest.raises(JoinProtocolError, match='identity conflicts'):
                _invoke_join_lifecycle(client, operation, identity)
        finally:
            client.close()
            stop.set()
            source_thread.join(1.0)

    assert dispatched == []


@pytest.mark.parametrize(
    'outcome',
    [
        b'',
        ConnectionResetError(),
        BrokenPipeError(),
        ConnectionAbortedError(errno.ECONNABORTED, 'connection aborted'),
        OSError(errno.ENOTCONN, 'socket is not connected'),
        OSError(errno.ESHUTDOWN, 'socket is shut down'),
    ],
    ids=['eof', 'reset', 'broken-pipe', 'aborted', 'not-connected', 'shutdown'],
)
def test_event_server_normalizes_unix_stream_receive_disconnects(
    outcome: bytes | OSError,
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )
    losses: list[tuple[str, str]] = []

    class _Source(_JoinSourceStub):
        def event_channel_lost(self, group: str, *, reason: str) -> None:
            losses.append((group, reason))

    read_peer = cast(
        'Callable[[EventServer, object], None]',
        vars(EventServer)['_read_peer'],
    )
    with server:
        _install_grant(server, 'group-a')
        client.connect()
        _wait_for_client_state(server, client, connected=True)
        peer = _server_peer(server, 'event')
        _script_peer_recv(peer, outcome)

        read_peer(server, peer)
        server.dispatch_join_requests(_Source())

        status = server._group_status('group-a')
        assert not status._connected
        assert status.queued_count == 0
        assert status.queued_bytes == 0
        assert status._reason == 'peer-eof'
        assert losses == [('group-a', 'event-channel-lost')]
    client.close()


@pytest.mark.parametrize(
    'split',
    ['prefix', 'payload', 'complete'],
)
def test_event_server_preserves_pipelined_coordination_authentication_bytes(
    split: Literal['prefix', 'payload', 'complete'],
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    request = JoinConfirm(
        LiveJoinIdentity(
            site='main',
            source='events',
            group='group-a',
            join_kind='eager',
            source_generation='source-1',
            group_generation=1,
            boundary=100,
            attempt_token='x' * 90_000,
        )
    )
    expected = Attached(request.identity)
    call = vars(ipc_module)['_JoinCall'](
        _request_id=1,
        _value=request,
    )
    encode_join_call = cast(
        'Callable[[object], _OutboundFrame]',
        vars(ipc_module)['_encode_join_call'],
    )
    request_frame = _frame_bytes(encode_join_call(call))
    assert 65_536 < len(request_frame) - 4 <= _MAX_FRAME_BYTES
    if split == 'prefix':
        split_at = 2
    elif split == 'payload':
        split_at = 4 + (len(request_frame) - 4) // 2
    else:
        split_at = len(request_frame)
    first = _event_authentication_frame('coordination') + request_frame[:split_at]
    remaining = request_frame[split_at:]
    received: list[JoinConfirm] = []

    class _Source(_JoinSourceStub):
        def confirm(self, confirmation: JoinConfirm) -> Attached:
            assert confirmation.identity.group == 'group-a'
            received.append(confirmation)
            return expected

    source = _Source()
    with (
        socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client,
        EventServer(
            path=path,
            source='events',
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=2,
            queue_max_bytes=_QUEUE_BYTES,
        ) as server,
    ):
        _install_grant(server, 'group-a')
        client.connect(os.fspath(path))
        client.settimeout(0.0)
        server.poll(0.001)
        peer = _server_peer(server, None)
        _write_event_server_stream(server, client, first)
        _service_event_server_until(
            server,
            lambda: vars(peer)['_role'] == 'coordination',
        )
        assert vars(peer)['_role'] == 'coordination'

        if remaining:
            partial_reads = 1 if split == 'prefix' else 2
            for _ in range(partial_reads):
                server.poll(0.001)
            _write_event_server_stream(server, client, remaining)

        _service_event_server_until(
            server,
            lambda: bool(received),
            source=source,
        )
        for _ in range(3):
            server.poll(0.0)
            server.dispatch_join_requests(source)

        assert received == [request]


@pytest.mark.parametrize(
    ('decoder_name', 'field_context', 'valid_mapping', 'valid_value'),
    [
        (
            '_decode_bootstrap_observation',
            (
                'source_successful_clock',
                'group bootstrap successful clock conflicts',
            ),
            {
                'captured_frontier': 41,
                'group': 'group-a',
                'group_generation': 7,
                'source': 'events',
                'source_generation': 'source-1',
                'source_successful_clock': 0.0,
                'stream_incarnation': 'incarnation-1',
            },
            0.0,
        ),
        (
            '_decode_bootstrap_pending',
            ('retry_after_secs', 'group bootstrap pending delay conflicts'),
            {
                'group': 'group-a',
                'group_generation': 7,
                'retry_after_secs': 60.0,
                'source': 'events',
            },
            60.0,
        ),
        (
            '_decode_join_retry',
            ('retry_after_secs', 'join retry bounds conflict'),
            {
                'attempt': {
                    'attempt_token': _ATTEMPT_ID,
                    'group': 'group-a',
                    'group_generation': 7,
                    'join_kind': 'strict',
                    'site': 'main',
                    'source': 'events',
                },
                'headroom_bytes': 1024,
                'headroom_events': 8,
                'retry_after_secs': 0.0,
            },
            0.0,
        ),
        (
            '_decode_join_target',
            (
                'source_successful_clock',
                'join target successful clock must be numeric or null',
            ),
            {
                'identity': {
                    'attempt_token': _ATTEMPT_ID,
                    'boundary': 20,
                    'group': 'group-a',
                    'group_generation': 7,
                    'join_kind': 'strict',
                    'site': 'main',
                    'source': 'events',
                    'source_generation': 'source-1',
                },
                'source_successful_clock': 0.0,
                'stream_incarnation': 'incarnation-1',
            },
            0.0,
        ),
    ],
)
@pytest.mark.parametrize(
    'invalid_value',
    [
        pytest.param(10**400, id='oversized-int'),
        pytest.param(1e400, id='infinity'),
    ],
)
def test_float_wire_decoders_reject_unrepresentable_and_non_finite_values(
    decoder_name: str,
    field_context: tuple[str, str],
    valid_mapping: dict[str, object],
    valid_value: float,
    invalid_value: object,
) -> None:
    decoder = cast(
        'Callable[[Mapping[str, object]], object]',
        vars(ipc_module)[decoder_name],
    )
    field, context = field_context

    decoded = decoder(valid_mapping)

    assert getattr(decoded, field) == valid_value
    with pytest.raises(_FrameError, match=context):
        decoder({**valid_mapping, field: invalid_value})


@pytest.mark.parametrize(
    ('payload', 'message'),
    [
        (
            {
                'evidence_id': 'evidence-1',
                'group': 'remote',
                'group_generation': 7,
                'outcome': 'parked',
                'resulting_revision': 12,
                'site': 'main',
                'spawn_instance': 'spawn-1',
            },
            None,
        ),
        (
            {
                'evidence_id': 'evidence-1',
                'extra': True,
                'group': 'remote',
                'group_generation': 7,
                'outcome': 'parked',
                'resulting_revision': 12,
                'site': 'main',
                'spawn_instance': 'spawn-1',
            },
            'fields',
        ),
        (
            {
                'evidence_id': 'token=must-not-cross-ipc',
                'group': 'remote',
                'group_generation': 7,
                'outcome': 'parked',
                'resulting_revision': 12,
                'site': 'main',
                'spawn_instance': 'spawn-1',
            },
            'secret-looking',
        ),
        (
            {
                'evidence_id': 'evidence-1',
                'group': 'remote',
                'group_generation': '7',
                'outcome': 'parked',
                'resulting_revision': 12,
                'site': 'main',
                'spawn_instance': 'spawn-1',
            },
            'generation',
        ),
        (
            {
                'evidence_id': 'evidence-1',
                'group': 'remote',
                'group_generation': 7,
                'resulting_revision': 12,
                'site': 'main',
                'spawn_instance': 'spawn-1',
            },
            'fields',
        ),
    ],
)
def test_group_recovery_ready_is_closed_versioned_and_identity_exact(
    payload: dict[str, object],
    message: str | None,
) -> None:
    ready_type = vars(ipc_module).get('GroupRecoveryReady')
    assert ready_type is not None
    if message is None:
        ready = ready_type(**payload)
        assert _decode_control_message(encode_control_message(ready)) == ready
        assert tuple(item.name for item in fields(ready_type)) == (
            'spawn_instance',
            'evidence_id',
            'site',
            'group',
            'group_generation',
            'outcome',
            'resulting_revision',
        )
        return
    body = json.dumps(
        {
            'kind': 'group-recovery-ready',
            'payload': payload,
            'schema': 'sg-event-daemon-worker-control',
            'version': _CONTROL_PROTOCOL_VERSION,
        },
        separators=(',', ':'),
    ).encode()
    frame = struct.pack('!I', len(body)) + body
    with pytest.raises(_FrameError, match=message):
        _decode_control_message(frame)

    duplicate = (
        b'{"kind":"group-recovery-ready","payload":{"spawn_instance":"spawn-1",'
        b'"evidence_id":"evidence-1","site":"main","site":"other","group":"remote",'
        b'"group_generation":7,"outcome":"parked","resulting_revision":12},'
        b'"schema":"sg-event-daemon-worker-control","version":1}'
    )
    with pytest.raises(_FrameError, match='duplicate'):
        _decode_control_message(struct.pack('!I', len(duplicate)) + duplicate)


@pytest.mark.parametrize(
    ('collection', 'index', 'field_name'),
    [
        ('watchdogs', 0, 'kind'),
        ('watchdogs', 0, 'site'),
        ('watchdogs', 0, 'source'),
        ('watchdogs', 0, 'group'),
        ('watchdogs', 0, 'worker_instance'),
        ('watchdogs', 0, 'phase'),
        ('watchdogs', 0, 'plugin_id'),
        ('watchdogs', 0, 'callback_id'),
        ('watchdogs', 0, 'operator_command'),
        ('worker', None, 'watchdog_failure'),
        ('sources', 0, 'overdue_reason'),
        ('groups', 0, 'overdue_reason'),
    ],
)
def test_health_update_rejects_malformed_new_text_fields(
    collection: str,
    index: int | None,
    field_name: str,
) -> None:
    encoded = encode_control_message(_new_text_health_update())
    envelope = cast('dict[str, object]', json.loads(encoded[4:]))
    payload = cast('dict[str, object]', envelope['payload'])
    if index is None:
        record = cast('dict[str, object]', payload[collection])
    else:
        records = cast('list[dict[str, object]]', payload[collection])
        record = records[index]
    record[field_name] = 7
    body = json.dumps(envelope, separators=(',', ':')).encode()

    with pytest.raises(_FrameError, match=r'health.*invalid'):
        _decode_control_message(struct.pack('!I', len(body)) + body)


@pytest.mark.parametrize('ordering', ['eager', 'strict'])
def test_joined_client_queue_pauses_and_resumes_socket_reads(
    ordering: Literal['eager', 'strict'],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=4,
        queue_max_bytes=_QUEUE_BYTES,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=1,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=30.0,
        stop=threading.Event(),
    )
    identity = LiveJoinIdentity(
        site='main',
        source='events',
        group='group-a',
        join_kind=ordering,
        source_generation='source-1',
        group_generation=1,
        boundary=60,
        attempt_token=_ATTEMPT_ID,
    )

    remotes: list[GroupChannelInterface] = []

    class _Source(_JoinSourceStub):
        def confirm(self, confirmation: JoinConfirm) -> Attached:
            assert confirmation == JoinConfirm(identity)
            return Attached(identity)

        def event_channel_lost(self, group: str, *, reason: str) -> None:
            del group
            assert reason == 'event-channel-lost'

        def request(
            self,
            request: JoinRequest,
            channel: GroupChannelInterface,
        ) -> JoinTarget:
            del request
            remotes.append(channel)
            return JoinTarget(identity)

    source = _Source()
    source_thread = _SourceThreadHarness(server, source)
    original_modify_peer = cast(
        'Callable[[EventServer, object], None]',
        vars(EventServer)['_modify_peer'],
    )
    monkeypatch.setattr(
        EventServer,
        '_modify_peer',
        _source_owner_guard(
            original_modify_peer,
            source_thread._owner_ident,
        ),
    )

    first = EventDelivery(EncodedEvent.from_event_frame(61, encode_event(_event(61))))
    second = EventDelivery(EncodedEvent.from_event_frame(62, encode_event(_event(62))))
    with server:
        _install_grant(server, 'group-a')
        source_thread._start()
        try:
            client.connect()
            channel = GroupChannel(
                group='group-a',
                ordering=ordering,
                max_events=1,
                max_bytes=_QUEUE_BYTES,
            )
            attempt = JoinAttemptIdentity(
                site='main',
                source='events',
                group='group-a',
                join_kind=ordering,
                group_generation=1,
                attempt_token=_ATTEMPT_ID,
            )
            assert client.request(JoinRequest(attempt), channel) == JoinTarget(identity)
            source_thread._call(
                lambda: _offer_joined_messages(
                    remotes[0],
                    ordering,
                    first,
                    second,
                )
            )

            for _ in range(100):
                client.poll(0.001)
                if channel.status.queued_count == 1:
                    break
            assert channel.status.queued_count == 1
            for _ in range(20):
                assert client.poll(0.001) == 0
                assert client.connected
                assert channel.status.attached
            assert _run_bounded_client_call(
                client,
                lambda: client.confirm(JoinConfirm(identity)),
            ) == Attached(identity)
            assert client.connected
            assert channel.status.attached
            assert channel.status.queued_count == 1
            assert channel.status.queued_bytes == first.byte_count
            assert channel.drain() == (first,)

            for _ in range(100):
                client.poll(0.001)
                if channel.status.queued_count == 1:
                    break
            assert client.connected
            assert channel.drain() == (second,)
        finally:
            client.close()
            source_thread._close()


@pytest.mark.parametrize(
    'split',
    ['prefix', 'payload', 'complete'],
)
def test_raw_frame_reader_handoff_preserves_unread_next_frame(
    split: Literal['prefix', 'payload', 'complete'],
) -> None:
    factory = ipc_module._RawFrameReader
    authentication = b'authentication'
    request = b'normal request'
    authentication_frame = struct.pack('!I', len(authentication)) + authentication
    request_frame = struct.pack('!I', len(request)) + request
    if split == 'prefix':
        split_at = 2
    elif split == 'payload':
        split_at = 4 + len(request) // 2
    else:
        split_at = len(request_frame)
    unread = request_frame[:split_at]
    remaining = request_frame[split_at:]
    authentication_reader = factory(max_frame_bytes=_MAX_FRAME_BYTES)

    assert authentication_reader._feed(
        authentication_frame + unread,
        frame_limit=1,
    ) == (authentication,)
    assert authentication_reader._handoff() == unread
    assert authentication_reader._handoff() == b''

    normal_reader = factory(max_frame_bytes=_MAX_FRAME_BYTES)
    decoded = normal_reader._feed(unread) + normal_reader._feed(remaining)
    assert decoded == (request,)


def test_worker_available_batch_yields_buffered_frame_before_snapshot_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sender, receiver = socket.socketpair()
    with (
        sender,
        closing(
            WorkerControl(receiver, max_frame_bytes=_MAX_FRAME_BYTES, send_deadline_secs=1)
        ) as control,
    ):
        first, second = StopCommand(reason='first'), StopCommand(reason='second')
        sender.sendall(encode_control_message(first) + encode_control_message(second))
        assert control.receive(timeout=0) == first
        failure = OSError(errno.EIO, 'injected inspection failure')
        inspected: list[socket.socket] = []

        def fail_snapshot(sock: socket.socket) -> int:
            inspected.append(sock)
            raise failure

        monkeypatch.setattr(ipc_module, 'pending_socket_bytes', fail_snapshot)
        batch = control.receive_available()
        assert inspected == [receiver]
        assert next(batch) == second
        with pytest.raises(EOFError) as caught:
            next(batch)
        assert caught.value.__cause__ is failure
        assert receiver.fileno() == -1


def test_worker_control_concurrent_partial_sends_keep_complete_frames() -> None:
    first = StopCommand(reason='first-1-' + 'a' * 32_768)
    second = StopCommand(reason='second-2-' + 'b' * 32_768)
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(
        parent_socket,
        max_frame_bytes=_MAX_FRAME_BYTES,
        send_deadline_secs=1.0,
    )
    child = WorkerControl(
        child_socket,
        max_frame_bytes=_MAX_FRAME_BYTES,
        send_deadline_secs=1.0,
    )
    vars(parent)['_socket'] = _SpuriousShortSendSocket(parent_socket, chunk_bytes=113)
    received: list[StopCommand] = []
    failures: list[BaseException] = []

    def receive_messages() -> None:
        try:
            received.extend(cast('StopCommand', child.receive(timeout=1.0)) for _ in range(2))
        except (EOFError, _FrameError, TimeoutError, TypeError, ValueError) as error:
            failures.append(error)

    receiver = threading.Thread(target=receive_messages)
    senders = (
        threading.Thread(target=parent.send, args=(first,)),
        threading.Thread(target=parent.send, args=(second,)),
    )
    try:
        receiver.start()
        for sender in senders:
            sender.start()
        for sender in senders:
            sender.join(2.0)
        receiver.join(2.0)

        assert not failures
        assert not receiver.is_alive()
        assert all(not sender.is_alive() for sender in senders)
        assert received in ([first, second], [second, first])
        assert parent_socket.getblocking() is False
        assert child_socket.getblocking() is False
    finally:
        parent.close()
        child.close()


def test_worker_control_honors_configured_limit_above_16_mib() -> None:
    message = StopCommand(reason='x' * (16 * 1024 * 1024))
    max_frame_bytes = 17 * 1024 * 1024
    frame = encode_control_message(message)
    assert 16 * 1024 * 1024 < len(frame) - 4 < max_frame_bytes
    assert _decode_control_message(frame, max_frame_bytes=max_frame_bytes) == message
    parent_socket, child_socket = socket.socketpair()
    control = WorkerControl(
        parent_socket,
        max_frame_bytes=max_frame_bytes,
        send_deadline_secs=5.0,
    )
    errors: list[OSError] = []

    def send_frame() -> None:
        try:
            child_socket.sendall(frame)
        except OSError as error:
            errors.append(error)

    sender = threading.Thread(target=send_frame)
    try:
        child_socket.settimeout(5.0)
        sender.start()
        assert control.receive(timeout=5.0) == message
        sender.join(5.0)
        assert not sender.is_alive()
        assert not errors
    finally:
        control.close()
        child_socket.close()
        sender.join(5.0)


def test_worker_control_partial_receive_survives_timeout_and_spurious_read() -> None:
    message = StopCommand(reason='partial-receive')
    frame = encode_control_message(message)
    parent_socket, child_socket = socket.socketpair()
    control = WorkerControl(
        parent_socket,
        max_frame_bytes=_MAX_FRAME_BYTES,
        send_deadline_secs=1.0,
    )
    vars(control)['_socket'] = _SpuriousRecvSocket(parent_socket)
    try:
        child_socket.sendall(frame[:7])
        with pytest.raises(TimeoutError, match='worker control receive timed out'):
            control.receive(timeout=0.02)

        child_socket.sendall(frame[7:])
        assert control.receive(timeout=1.0) == message
        assert parent_socket.getblocking() is False
    finally:
        control.close()
        child_socket.close()


@pytest.mark.parametrize('operation', ['receive', 'send'])
def test_worker_control_persistent_spurious_readiness_keeps_deadline(
    operation: Literal['receive', 'send'],
) -> None:
    parent_socket, child_socket = socket.socketpair()
    control = WorkerControl(
        parent_socket,
        max_frame_bytes=_MAX_FRAME_BYTES,
        send_deadline_secs=0.05,
    )
    if operation == 'receive':
        child_socket.sendall(b'x')
    vars(control)['_socket'] = _AlwaysBlockingSocket(parent_socket)
    errors: list[Exception] = []
    returned = threading.Event()

    def operate() -> None:
        try:
            if operation == 'send':
                control.send(StopCommand(reason='persistent-spurious-write'))
            else:
                control.receive(timeout=0.05)
        except (EOFError, OSError, ValueError) as error:
            errors.append(error)
        else:
            returned.set()

    started = time.monotonic()
    thread = threading.Thread(target=operate)
    thread.start()
    thread.join(0.55)
    elapsed = time.monotonic() - started
    if thread.is_alive():
        control.close()
        thread.join(1.0)
    try:
        assert not thread.is_alive()
        assert not returned.is_set()
        assert len(errors) == 1
        assert isinstance(errors[0], TimeoutError)
        assert elapsed < 0.55
    finally:
        control.close()
        child_socket.close()


@pytest.mark.parametrize('fault', [0, 1, 2])
def test_client_close_attempts_all_detached_resources(tmp_path: Path, fault: int) -> None:
    attempts: list[int] = []
    failure = OSError('detach cleanup failed')

    class Resource:
        def __init__(self, index: int) -> None:
            self.index = index

        def close(self) -> None:
            attempts.append(self.index)
            if self.index == fault:
                raise failure

    client = EventClient(
        path=tmp_path / 'events.sock',
        source='events',
        group='remote',
        spawn_instance='spawn',
        capability='capability',
        max_frame_bytes=131_072,
        queue_max_entries=8,
        queue_max_bytes=262_144,
        coordination_deadline_secs=1.0,
        stop=threading.Event(),
    )
    names = ('_selector', '_coordination_selector', '_socket', '_coordination_socket')
    vars(client).update(zip(names, (Resource(i) for i in range(4)), strict=True))
    channel = GroupChannel(group='remote', ordering='eager', max_events=8, max_bytes=_QUEUE_BYTES)
    assert channel.offer(
        EventDelivery(EncodedEvent.from_event_frame(21, encode_event(_event(21))))
    )
    assert channel.status.queued_bytes > 0
    vars(client)['_join_channel'] = channel
    with pytest.raises(OSError, match='fail') as caught:
        client.close()
    assert caught.value is failure
    assert attempts == [0, 1, 2, 3]
    assert all(vars(client)[name] is None for name in names)
    assert channel.status.queued_bytes == 0
    assert channel.status.queued_count == 0
    assert not channel.status.attached
    assert client.detach_reason == 'client-closed'
    assert not client.connected


@pytest.mark.parametrize('replacement', ['none', 'file', 'socket'])
def test_server_close_attempts_listener_and_preserves_path_identity(
    tmp_path: Path,
    replacement: Literal['none', 'file', 'socket'],
) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=131_072,
        queue_max_entries=8,
        queue_max_bytes=262_144,
    )
    server.__enter__()
    listener = vars(server)['_listener']
    selector = vars(server)['_selector']
    failure = OSError('selector cleanup failed')

    def close() -> None:
        selector.close()
        raise failure

    vars(server)['_selector'] = SimpleNamespace(close=close)
    replacement_identity: tuple[int, int] | None = None
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as replacement_socket:
        if replacement != 'none':
            path.unlink()
            if replacement == 'socket':
                replacement_socket.bind(os.fspath(path))
            else:
                path.write_text('replacement')
            metadata = path.lstat()
            replacement_identity = (metadata.st_dev, metadata.st_ino)
            assert replacement_identity != vars(server)['_bound_identity']
        try:
            with pytest.raises(OSError, match='selector cleanup failed') as caught:
                server.close()
            assert caught.value is failure
            assert listener.fileno() == -1
            assert path.exists() is (replacement != 'none')
            assert vars(server)['_bound_identity'] is None
            if replacement != 'none':
                metadata = path.lstat()
                assert (metadata.st_dev, metadata.st_ino) == replacement_identity
        finally:
            listener.close()
            path.unlink(missing_ok=True)


@pytest.mark.parametrize('fault', ['unregister', 'socket'])
def test_server_close_attempts_siblings_after_peer_cleanup_failure(
    tmp_path: Path,
    fault: str,
) -> None:
    path = _socket_path(tmp_path)
    server = EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    )
    client = EventClient(
        path=path,
        source='events',
        group='group-a',
        spawn_instance='spawn-1',
        capability='capability-1',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
        coordination_deadline_secs=1.0,
        stop=threading.Event(),
    )
    server.start()
    _install_grant(server, 'group-a')
    client.connect()
    for _ in range(100):
        server.poll(0.001)
        if server._connected_groups:
            break
    assert server._connected_groups == ('group-a',)
    peers = tuple(vars(server)['_peers'].values())
    assert len(peers) == 2
    sockets = tuple(peer._socket for peer in peers)
    selector = vars(server)['_selector']
    failure = OSError('peer cleanup failed')
    attempts: list[str] = []

    def unregister(sock: socket.socket) -> None:
        attempts.append('unregister')
        selector.unregister(sock)
        raise failure

    class Socket:
        def fileno(self) -> int:
            return sockets[0].fileno()

        def close(self) -> None:
            sockets[0].close()
            raise failure

    if fault == 'unregister':
        vars(server)['_selector'] = SimpleNamespace(unregister=unregister, close=selector.close)
    else:
        peers[0]._socket = Socket()
    try:
        with pytest.raises(OSError, match='peer cleanup failed') as caught:
            server.close()
        assert caught.value is failure
        assert all(sock.fileno() == -1 for sock in sockets)
        assert not vars(server)['_peers']
        assert server._connected_groups == ()
        assert not path.exists()
        assert vars(server)['_connection_fences']['group-a']._retired
        assert list(vars(server)['_lost_groups']) == ['group-a']
    finally:
        client.close()
        for sock in sockets:
            sock.close()
        selector.close()
        listener = vars(server)['_listener']
        if listener is not None:
            listener.close()
        path.unlink(missing_ok=True)


@pytest.mark.parametrize(
    ('decoder', 'field_name', 'attribute_name'),
    [
        ('_decode_auth_body', 'role', '_role'),
        ('_decode_join_control', 'action', 'action'),
        ('_decode_attempt', 'join_kind', 'join_kind'),
        ('_decode_bootstrap_request', 'start_mode', 'start_mode'),
        ('_wire_identity', 'join_kind', 'join_kind'),
        ('_decode_strict_ordering', 'action', 'action'),
    ],
)
@pytest.mark.parametrize(
    'invalid', [[], {}, 'invalid-enum', None], ids=['array', 'object', 'unknown-string', 'valid']
)
def test_enum_decoders_preserve_protocol_rejection(
    decoder: str,
    field_name: str,
    attribute_name: str,
    invalid: object,
) -> None:
    payload = _enum_payload(decoder)
    if invalid is None:
        decoded = _decode_enum_payload(decoder, payload)
        assert vars(decoded)[attribute_name] == payload[field_name]
    else:
        payload[field_name] = invalid
        error = _IPCAuthenticationError if decoder == '_decode_auth_body' else _FrameError
        with pytest.raises(error):
            _decode_enum_payload(decoder, payload)


@pytest.mark.parametrize('role', [[], {}, 'invalid-enum'])
def test_event_server_rejects_malformed_auth_then_services_valid_peer(
    role: object,
    tmp_path: Path,
) -> None:
    path = _socket_path(tmp_path)
    with EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    ) as server:
        _install_grant(server, 'group-a')
        payload = cast('dict[str, object]', json.loads(_event_authentication_frame('event')[4:]))
        payload['role'] = role
        body = json.dumps(payload).encode()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as malformed:
            malformed.connect(os.fspath(path))
            malformed.sendall(struct.pack('!I', len(body)) + body)
            for _ in range(10):
                server.poll(0.001)
            malformed.settimeout(1.0)
            assert malformed.recv(1) == b''
        client = EventClient(
            path=path,
            source='events',
            group='group-a',
            spawn_instance='spawn-1',
            capability='capability-1',
            max_frame_bytes=_MAX_FRAME_BYTES,
            queue_max_entries=2,
            queue_max_bytes=_QUEUE_BYTES,
            coordination_deadline_secs=30.0,
            stop=threading.Event(),
        )
        try:
            client.connect()
            _wait_for_client_state(server, client, connected=True)
            channel, remote = _join_client(server, client)
            assert remote.offer(
                EventDelivery(EncodedEvent.from_event_frame(21, encode_event(_event(21))))
            )
            for _ in range(10):
                server.poll(0.001)
                client.poll(0.001)
            (delivered,) = channel.drain()
            assert isinstance(delivered, EventDelivery)
            assert delivered.encoded.event_id == 21
        finally:
            client.close()


@pytest.mark.parametrize('kind', ['capacity', 'ordering'])
def test_strict_control_envelopes_keep_exact_fields_and_shared_values(kind: str) -> None:
    identity = LiveJoinIdentity(
        site='main',
        source='events',
        group='group-a',
        join_kind='strict',
        source_generation='source-1',
        group_generation=1,
        boundary=20,
        attempt_token=_ATTEMPT_ID,
    )
    control = (
        StrictCapacityControl(identity=identity, abort_limit=4)
        if kind == 'capacity'
        else StrictOrderingControl(
            identity=identity, event_id=21, last_delivered_id=20, delivery_floor=20, action='skip'
        )
    )
    frame = _encode_fanout_message(control)
    fanout = cast('dict[str, object]', json.loads(frame._header)['metadata'])
    acknowledge = {**fanout, 'control_kind': kind, 'request_id': 1}
    decode_ack = cast(
        'Callable[[dict[str, object], LiveJoinIdentity], object]',
        vars(ipc_module)['_decode_join_control'],
    )
    decode_fanout = cast(
        'Callable[[dict[str, object]], object]', vars(ipc_module)[f'_decode_strict_{kind}']
    )
    assert decode_ack(acknowledge, identity) == decode_fanout(fanout) == control
    with pytest.raises(_FrameError, match='fields'):
        decode_fanout(acknowledge)
    with pytest.raises(_FrameError, match='kind conflicts'):
        decode_ack(fanout, identity)
    for key in ('request_id', 'control_kind'):
        with pytest.raises(_FrameError, match='fields'):
            decode_fanout({**fanout, key: acknowledge[key]})
    invalid_fields = (
        [('abort_limit', 0), ('abort_limit', True)]
        if kind == 'capacity'
        else [
            ('event_id', 0),
            ('event_id', True),
            ('delivery_floor', -1),
            ('delivery_floor', True),
            ('last_delivered_id', -1),
            ('last_delivered_id', True),
        ]
    )
    for field_name, invalid in invalid_fields:
        with pytest.raises(_FrameError) as ack_error:
            decode_ack({**acknowledge, field_name: invalid}, identity)
        with pytest.raises(_FrameError) as fanout_error:
            decode_fanout({**fanout, field_name: invalid})
        assert str(ack_error.value) == str(fanout_error.value)


@pytest.mark.parametrize(
    ('component', 'field_name', 'invalid'),
    [
        ('worker', 'monitor_alive', 'false'),
        ('worker', 'breaker_open', 'false'),
        ('worker', 'site', 1),
        ('worker', 'worker_id', []),
        ('worker', 'spawn_instance', {}),
        ('worker', 'breaker_reason', 1),
        ('worker', 'watchdog_failure', False),
        ('worker', 'heartbeat_at', True),
        ('worker', 'heartbeat_age', '10'),
        ('worker', 'socket_buffer_requested', True),
        ('worker', 'socket_send_buffer', 1.5),
        ('worker', 'socket_receive_buffer', '1'),
        ('worker', 'thread_names', 'thread'),
        ('worker', 'thread_names', [1]),
        ('worker', 'stuck_threads', [dict[str, object]()]),
        ('worker', 'source_outages', [['events']]),
        ('worker', 'source_outages', [['events', 'failed', 'extra']]),
        ('worker', 'source_outages', ['xy']),
        ('worker', 'source_outages', [[1, 'failed']]),
        ('worker', 'source_outages', [['events', None]]),
        ('watchdogs', 'kind', []),
        ('watchdogs', 'kind', {}),
        ('watchdogs', 'kind', 'invalid'),
        ('watchdogs', 'phase', []),
        ('watchdogs', 'phase', {}),
        ('watchdogs', 'phase', 'invalid'),
        ('watchdogs', 'site', 1),
        ('watchdogs', 'source', []),
        ('watchdogs', 'group', 1),
        ('watchdogs', 'worker_instance', False),
        ('watchdogs', 'heartbeat_at', True),
        ('watchdogs', 'deadline_at', '20'),
        ('watchdogs', 'event_id', True),
        ('watchdogs', 'delivery_attempt', True),
        ('watchdogs', 'plugin_id', 1),
        ('watchdogs', 'callback_id', []),
        ('watchdogs', 'operator_command', {}),
        ('sources', 'initialized', 'false'),
        ('sources', 'status', []),
        ('sources', 'status', {}),
        ('sources', 'status', 'invalid'),
        ('sources', 'phase', []),
        ('sources', 'phase', {}),
        ('sources', 'phase', 'invalid'),
        ('sources', 'source', 1),
        ('sources', 'stream_incarnation', False),
        ('sources', 'last_error', 1),
        ('sources', 'frontier', True),
        ('sources', 'retry_attempt', True),
        ('sources', 'audit_count', True),
        ('sources', 'database_size_bytes', True),
        ('sources', 'successful_clock', True),
        ('sources', 'next_retry_secs', '10'),
        ('sources', 'strict_retention', 'x123'),
        ('sources', 'strict_retention', ['x123']),
        ('sources', 'strict_retention', [['group', 1, 2]]),
        ('sources', 'strict_retention', [['group', 1, 2, 3, 4]]),
        ('sources', 'strict_retention', [[1, 1, 2, 3]]),
        ('sources', 'strict_retention', [['group', True, 2, 3]]),
        ('sources', 'strict_retention', [['group', 1, False, 3]]),
        ('sources', 'strict_retention', [['group', 1, 2, '3']]),
        ('groups', 'execution', []),
        ('groups', 'execution', {}),
        ('groups', 'execution', 'invalid'),
        ('groups', 'ordering', []),
        ('groups', 'ordering', {}),
        ('groups', 'ordering', 'invalid'),
        ('groups', 'phase', []),
        ('groups', 'phase', {}),
        ('groups', 'phase', 'invalid'),
        ('groups', 'parked', 'false'),
        ('groups', 'outstanding_late_ids_truncated', 'false'),
        ('groups', 'group', 1),
        ('groups', 'status', []),
        ('groups', 'failure_text', 1),
        ('groups', 'cursor', True),
        ('groups', 'current_event_id', True),
        ('groups', 'delivery_attempt', 1.5),
        ('groups', 'current_plugin_id', 1),
        ('groups', 'current_callback_id', []),
        ('groups', 'catchup_frontier', True),
        ('groups', 'oldest_catchup_epoch_age', '1'),
        ('groups', 'audit_count', True),
        ('groups', 'outstanding_late_ids', '1'),
        ('groups', 'outstanding_late_ids', [[1]]),
        ('groups', 'conditions', 'healthy'),
        ('groups', 'conditions', [dict[str, object]()]),
        ('event_sockets', 'connected', 'false'),
        ('event_sockets', 'role', []),
        ('event_sockets', 'role', {}),
        ('event_sockets', 'role', 'invalid'),
        ('event_sockets', 'source', 1),
        ('event_sockets', 'group', False),
        ('event_sockets', 'reason', 1),
        ('event_sockets', 'requested', True),
        ('event_sockets', 'send', True),
        ('event_sockets', 'receive', True),
        ('event_sockets', 'queue_event_count', True),
        ('event_sockets', 'queue_byte_count', 1.5),
    ],
)
def test_health_frame_rejects_malformed_typed_fields(
    component: str,
    field_name: str,
    invalid: object,
) -> None:
    envelope = cast(
        'dict[str, object]', json.loads(encode_control_message(_complete_health_update())[4:])
    )
    payload = cast('dict[str, object]', envelope['payload'])
    record = (
        cast('dict[str, object]', payload[component])
        if component == 'worker'
        else cast('list[dict[str, object]]', payload[component])[0]
    )
    record[field_name] = invalid
    body = json.dumps(envelope).encode()
    with pytest.raises(_FrameError):
        _decode_control_message(struct.pack('!I', len(body)) + body)


@pytest.mark.parametrize('group_count', [2, 13])
def test_connected_group_health_statuses_visit_each_peer_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    group_count: int,
) -> None:
    path = _socket_path(tmp_path)
    groups = tuple(f'group-{index:02d}' for index in reversed(range(group_count)))
    clients: list[EventClient] = []
    with EventServer(
        path=path,
        source='events',
        max_frame_bytes=_MAX_FRAME_BYTES,
        queue_max_entries=2,
        queue_max_bytes=_QUEUE_BYTES,
    ) as server:
        try:
            for group in groups:
                _install_grant(server, group)
                client = _event_client(path, group)
                clients.append(client)
                client.connect()
                _wait_for_client_state(server, client, connected=True)
            _service_event_server_until(
                server,
                lambda: server._connected_groups == tuple(sorted(groups)),
            )
            peers = server._peers
            visits = 0

            def counted_values() -> Iterator[object]:
                nonlocal visits
                for peer in peers.values():
                    visits += 1
                    yield peer

            expected = tuple(server._group_status(group) for group in sorted(groups))
            with monkeypatch.context() as patch:
                patch.setattr(server, '_peers', SimpleNamespace(values=counted_values))
                assert server.connected_group_statuses() == expected
            assert visits == 2 * group_count

            _channel, remote = _join_client(server, clients[0], group=groups[0])
            remote.detach('source-disconnected')
            statuses = server.connected_group_statuses()
            detached = next(item for item in statuses if item._group == groups[0])
            assert not detached._connected
            assert len(statuses) == group_count

            clients[0].close()
            _service_event_server_until(
                server,
                lambda: not server._group_status(groups[0])._connected,
            )
            statuses = server.connected_group_statuses()
            assert groups[0] not in tuple(item._group for item in statuses)
            server.install_grant(
                ipc_module.EventPeerGrant(
                    source='events',
                    group=groups[1],
                    dependent_spawn_instance='replacement',
                    capability='replacement-key',
                )
            )
            statuses = server.connected_group_statuses()
            assert tuple(item._group for item in statuses) == tuple(sorted(groups[2:]))
            assert all(item._connected for item in statuses)
        finally:
            for client in clients:
                client.close()


def test_worker_control_pretransport_failures_keep_channel_usable() -> None:
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(
        parent_socket,
        max_frame_bytes=256,
        send_deadline_secs=1.0,
    )
    child = WorkerControl(
        child_socket,
        max_frame_bytes=256,
        send_deadline_secs=1.0,
    )
    try:
        with pytest.raises(_FrameError, match='exceeds'):
            parent.send(StopCommand(reason='x' * 512))
        with pytest.raises((AttributeError, TypeError, ValueError)):
            parent.send(cast('StopCommand', object()))

        message = StopCommand(reason='still-usable')
        parent.send(message)
        assert child.receive(timeout=1.0) == message
    finally:
        parent.close()
        child.close()


def test_worker_control_receive_eof_permanently_poisons_channel() -> None:
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(
        parent_socket,
        max_frame_bytes=4_096,
        send_deadline_secs=1.0,
    )
    child_socket.close()
    try:
        with pytest.raises(EOFError, match='worker control peer closed'):
            parent.receive(timeout=1.0)
        with pytest.raises(EOFError, match='worker control is closed'):
            parent.receive(timeout=0.0)
    finally:
        parent.close()


def test_worker_control_rejects_oversized_length_before_body_allocation() -> None:
    max_frame_bytes = 17 * 1024 * 1024
    parent_socket, child_socket = socket.socketpair()
    control = WorkerControl(
        parent_socket,
        max_frame_bytes=max_frame_bytes,
        send_deadline_secs=1.0,
    )
    header = struct.pack('!I', max_frame_bytes + 1)
    try:
        child_socket.sendall(header)
        with pytest.raises(_FrameError, match='exceeds configured maximum'):
            control.receive(timeout=1.0)
        # A length-only peer is rejected without reserving its advertised body.
        assert vars(vars(control)['_reader'])['_buffer'] == bytearray(header)
    finally:
        control.close()
        child_socket.close()


def test_worker_control_rejects_unrepresentable_conditional_park_timestamp() -> None:
    directive = ConditionalParkDirective(
        evidence_id='evidence-1',
        site='main',
        group='local',
        group_generation=7,
        failed_spawn_instance='failed-spawn-1',
        event_id=42,
        group_revision=11,
        observed_at_wall_clock=1234.5,
    )
    assignment = WorkerAssignment(
        worker_id='["site","main"]',
        spawn_instance='source-spawn-1',
        source_generations=(('events', 'source-generation-1'),),
        group_assignments=(
            ipc_module.DeliveryGroupAssignment(
                source='events',
                group='local',
                group_generation=7,
                start_mode='initialize',
                conditional_park=directive,
            ),
        ),
        source_group_assignments=(
            ActiveSourceGroupAssignment(
                source='events',
                group='local',
                group_generation=7,
                start_mode='initialize',
            ),
        ),
        source_endpoints=(),
        event_grants=(),
    )
    envelope = cast(
        'dict[str, object]',
        json.loads(encode_control_message(assignment)[4:]),
    )
    payload = cast('dict[str, object]', envelope['payload'])
    group_assignments = cast('list[object]', payload['group_assignments'])
    group_assignment = cast('dict[str, object]', group_assignments[0])
    conditional_park = cast('dict[str, object]', group_assignment['conditional_park'])
    conditional_park['observed_at_wall_clock'] = 10**400
    body = json.dumps(envelope, separators=(',', ':')).encode()
    frame = struct.pack('!I', len(body)) + body
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=4096, send_deadline_secs=1.0)
    try:
        child_socket.sendall(frame)

        with pytest.raises(
            _FrameError,
            match='conditional park directive observation time is invalid',
        ):
            parent.receive(timeout=1.0)

        assert vars(parent)['_messages'] == deque()
        child_socket.sendall(encode_control_message(StopCommand(reason='still-closed')))
        assert parent.receive(timeout=1.0) == StopCommand(reason='still-closed')
    finally:
        child_socket.close()
        parent.close()


def test_worker_control_send_deadline_closes_saturated_channel() -> None:
    message = StopCommand(reason='saturated-' + 'x' * _MAX_FRAME_BYTES)
    frame = encode_control_message(message)
    parent_socket, child_socket = socket.socketpair()
    control = WorkerControl(
        parent_socket,
        max_frame_bytes=len(frame) - 4,
        send_deadline_secs=0.05,
    )
    parent_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4_096)
    try:
        started = time.monotonic()
        with pytest.raises(TimeoutError, match='worker control send timed out'):
            control.send(message)
        assert time.monotonic() - started < 0.55
        with pytest.raises(EOFError, match='worker control is closed'):
            control.send(StopCommand(reason='must-not-resume-partial-frame'))
        with pytest.raises(EOFError, match='worker control is closed'):
            control.receive(timeout=0.0)
    finally:
        control.close()
        child_socket.close()


def test_worker_control_send_deadline_includes_lock_contention() -> None:
    parent_socket, child_socket = socket.socketpair()
    control = WorkerControl(
        parent_socket,
        max_frame_bytes=_MAX_FRAME_BYTES,
        send_deadline_secs=0.05,
    )
    send_lock = cast('threading.Lock', vars(control)['_send_lock'])
    send_lock.acquire()
    try:
        started = time.monotonic()
        with pytest.raises(TimeoutError, match='worker control send timed out'):
            control.send(StopCommand(reason='lock-contention'))
        assert time.monotonic() - started < 0.55
    finally:
        send_lock.release()
        child_socket.close()
    with pytest.raises(EOFError, match='worker control is closed'):
        control.send(StopCommand(reason='closed-after-contention'))
