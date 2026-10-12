from __future__ import annotations

import argparse
import asyncio
import json
from importlib import import_module, metadata
from types import MethodType
from typing import TYPE_CHECKING, Protocol, cast

from sg_event_daemon._config import CredentialConfig, SecretValue
from sg_event_daemon._events import EVENT_FIELDS
from sg_event_daemon._providers import (
    AsyncSG,
    Provider,
    SyncSG,
    _installed_provider_names,
    load_provider,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

_EVENT: dict[str, object] = {
    'attribute_name': None,
    'created_at': '2026-07-25T01:02:03Z',
    'entity': None,
    'event_type': 'Shotgun_Task_Change',
    'id': 1,
    'meta': {},
    'project': None,
    'session_uuid': None,
    'type': 'EventLogEntry',
    'user': None,
}
_FILTERS: list[list[object]] = [['id', 'greater_than', 0]]
_ORDER: list[dict[str, str]] = [{'field_name': 'id', 'direction': 'asc'}]
_SESSION_UUID = 'provider-contract-session'
_SITE = 'https://provider-contract.invalid'


class _Config(Protocol):
    max_rpc_attempts: int
    rpc_attempt_interval: int
    timeout_secs: float


class _Handle(Protocol):
    config: _Config


class _OfficialConnection:
    def __init__(self) -> None:
        self._closed = False

    def close(self) -> None:
        self._closed = True


class _OfficialPool:
    def __init__(self, connection: _OfficialConnection) -> None:
        self.connections = {'contract': connection}


class _OfficialResponder:
    def __init__(self) -> None:
        self._requests: list[dict[str, object]] = []

    def request(
        self,
        _handle: object,
        _verb: str,
        _path: str,
        body: bytes | str | None,
        _headers: object,
    ) -> tuple[tuple[int, str], dict[str, str], str]:
        if not isinstance(body, bytes | str):
            raise TypeError('official provider emitted a body-free request')
        payload = cast('dict[str, object]', json.loads(body))
        self._requests.append(payload)
        result = _response_result(payload)
        return (200, 'OK'), {'content-type': 'application/json'}, json.dumps({'results': result})


class _Request(Protocol):
    body: bytes | None


class _LocalAsyncTransport:
    def __init__(
        self,
        response_factory: Callable[..., object],
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._checkins = 0
        self._checkouts = 0
        self._closed = False
        self._loop = loop
        self._requests: list[dict[str, object]] = []
        self._response_factory = response_factory

    def checkin(self) -> None:
        self._checkins += 1

    def checkout(self) -> None:
        self._checkouts += 1

    async def close(self) -> None:
        assert asyncio.get_running_loop() is self._loop
        self._closed = True

    async def send(self, request: _Request) -> object:
        assert asyncio.get_running_loop() is self._loop
        payload = _decode_request(request)
        self._requests.append(payload)
        return self._response_factory(**_response_arguments(payload))

    async def send_streaming(self, _request: _Request) -> object:
        raise AssertionError('provider contract did not expect a streaming request')


class _LocalTransport:
    def __init__(self, response_factory: Callable[..., object]) -> None:
        self._closed = False
        self._requests: list[dict[str, object]] = []
        self._response_factory = response_factory

    def abort(self, _token: str) -> None:
        pass

    def close(self) -> None:
        self._closed = True

    def send(self, request: _Request) -> object:
        payload = _decode_request(request)
        self._requests.append(payload)
        return self._response_factory(**_response_arguments(payload))

    def send_streaming(self, _request: _Request) -> object:
        raise AssertionError('provider contract did not expect a streaming request')


def _assert_common_settings(handle: object) -> None:
    config = cast('_Handle', handle).config
    assert config.timeout_secs == 17.0
    assert config.max_rpc_attempts == 4
    assert config.rpc_attempt_interval == 2_500


def _assert_read_requests(requests: Sequence[dict[str, object]]) -> None:
    reads = [request for request in requests if request.get('method_name') == 'read']
    assert len(reads) == 2
    for request in reads:
        params = cast('list[dict[str, object]]', request['params'])
        operation = params[-1]
        assert operation['type'] == 'EventLogEntry'
        assert operation['return_fields'] == list(EVENT_FIELDS)
        assert operation['sorts'] == _ORDER
    first_auth = cast('list[dict[str, object]]', reads[0]['params'])[0]
    second_auth = cast('list[dict[str, object]]', reads[1]['params'])[0]
    assert first_auth['session_uuid'] == _SESSION_UUID
    assert second_auth.get('session_uuid') is None


def _assert_result(records: list[dict[str, object]]) -> None:
    assert len(records) == 1
    record = records[0]
    assert record['id'] == 1
    assert record['type'] == 'EventLogEntry'
    assert all(field in record for field in EVENT_FIELDS)


def _credential() -> CredentialConfig:
    return CredentialConfig(
        name='contract',
        script_name='provider-contract',
        api_key=SecretValue('not-a-live-key'),
        timeout_secs=17.0,
        max_rpc_attempts=4,
        rpc_attempt_interval_secs=2.5,
    )


def _decode_request(request: _Request) -> dict[str, object]:
    if not isinstance(request.body, bytes):
        raise TypeError('local provider emitted a non-bytes request')
    return cast('dict[str, object]', json.loads(request.body))


def _exercise_find(handle: SyncSG) -> None:
    handle.set_session_uuid(_SESSION_UUID)
    _assert_result(
        handle.find(
            'EventLogEntry',
            _FILTERS,
            list(EVENT_FIELDS),
            order=_ORDER,
            limit=1,
            page=1,
        )
    )
    handle.set_session_uuid(None)
    _assert_result(
        handle.find(
            'EventLogEntry',
            _FILTERS,
            list(EVENT_FIELDS),
            order=_ORDER,
            limit=1,
            page=1,
        )
    )


async def _exercise_find_async(handle: AsyncSG) -> None:
    handle.set_session_uuid(_SESSION_UUID)
    _assert_result(
        await handle.find(
            'EventLogEntry',
            _FILTERS,
            list(EVENT_FIELDS),
            order=_ORDER,
            limit=1,
            page=1,
        )
    )
    handle.set_session_uuid(None)
    _assert_result(
        await handle.find(
            'EventLogEntry',
            _FILTERS,
            list(EVENT_FIELDS),
            order=_ORDER,
            limit=1,
            page=1,
        )
    )


def _inject_official_responder(handle: object, responder: _OfficialResponder) -> None:
    object.__setattr__(handle, '_http_request', MethodType(responder.request, handle))


def _local_response_factory() -> Callable[..., object]:
    transport_module = import_module('shotgun_api3._transport')
    return cast('Callable[..., object]', transport_module.TransportResponse)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--provider',
        choices=('sg-python-api', 'shotgun-api3'),
        required=True,
    )
    parser.add_argument('--require-async', action='store_true')
    return parser.parse_args()


def _response_result(payload: dict[str, object]) -> object:
    method_name = payload.get('method_name')
    if method_name == 'info':
        return {
            'full_version': [9, 0, 0, 0],
            'records_per_page_limit': 500,
            'version': [9, 0, 0],
        }
    if method_name == 'read':
        return {
            'entities': [_EVENT],
            'paging_info': {'entity_count': 1, 'has_next_page': False},
        }
    raise AssertionError(f'unexpected provider RPC: {method_name!r}')


def _response_arguments(payload: dict[str, object]) -> dict[str, object]:
    return {
        'content': json.dumps({'results': _response_result(payload)}).encode(),
        'headers': {'content-type': 'application/json'},
        'reason': 'OK',
        'status': 200,
    }


async def _run_local_async(provider: Provider) -> None:
    loop = asyncio.get_running_loop()
    async_shotgun = import_module('shotgun_api3').AsyncShotgun
    handles = [await provider.create_async(_SITE, _credential()) for _index in range(3)]
    assert len({id(handle) for handle in handles}) == 3
    assert all(type(handle) is async_shotgun for handle in handles)
    assert async_shotgun.__module__ == 'shotgun_api3._async_client'
    assert all(object.__getattribute__(handle, '_transport') is None for handle in handles)
    transports = [_LocalAsyncTransport(_local_response_factory(), loop) for _handle in handles]
    for handle, transport in zip(handles, transports, strict=True):
        _assert_common_settings(handle)
        object.__setattr__(handle, '_transport', transport)
    await _exercise_find_async(handles[0])
    _assert_read_requests(transports[0]._requests)
    for handle in handles:
        await handle.close()
    assert all(transport._closed for transport in transports)
    assert all(transport._checkouts == transport._checkins for transport in transports)
    assert all(object.__getattribute__(handle, '_transport') is None for handle in handles)


def _run_local_sync(provider: Provider) -> None:
    handles = [provider.create_sync(_SITE, _credential()) for _index in range(3)]
    assert len({id(handle) for handle in handles}) == 3
    assert all(object.__getattribute__(handle, '_transport') is None for handle in handles)
    transports = [_LocalTransport(_local_response_factory()) for _handle in handles]
    for handle, transport in zip(handles, transports, strict=True):
        _assert_common_settings(handle)
        object.__setattr__(handle, '_transport', transport)
    _exercise_find(handles[0])
    _assert_read_requests(transports[0]._requests)
    for handle in handles:
        handle.close()
    assert all(transport._closed for transport in transports)
    assert all(object.__getattribute__(handle, '_transport') is None for handle in handles)


def _run_official_sync(provider: Provider) -> None:
    handles = [provider.create_sync(_SITE, _credential()) for _index in range(3)]
    assert len({id(handle) for handle in handles}) == 3
    assert all(object.__getattribute__(handle, '_connection') is None for handle in handles)
    responders = [_OfficialResponder() for _handle in handles]
    for handle, responder in zip(handles, responders, strict=True):
        _assert_common_settings(handle)
        _inject_official_responder(handle, responder)
    _exercise_find(handles[0])
    _assert_read_requests(responders[0]._requests)
    connections = [_OfficialConnection() for _handle in handles]
    for handle, connection in zip(handles, connections, strict=True):
        object.__setattr__(handle, '_connection', _OfficialPool(connection))
        handle.close()
    assert all(connection._closed for connection in connections)
    assert all(object.__getattribute__(handle, '_connection') is None for handle in handles)


def _main() -> None:
    args = _parse_args()
    provider_name = cast('str', args.provider)
    require_async = cast('bool', args.require_async)
    names = _installed_provider_names()
    assert names == (provider_name,), f'isolated provider required, found {names}'
    required = {'async', 'sync'} if require_async else {'sync'}
    provider = load_provider(provider_name, required)
    assert provider.name == provider_name
    if provider_name == 'shotgun-api3':
        _run_official_sync(provider)
    else:
        _run_local_sync(provider)
    collected = 1
    real_async_shotgun = 0
    if require_async:
        asyncio.run(_run_local_async(provider))
        collected += 1
        real_async_shotgun = 1
    version = metadata.version(provider_name)
    modes = 'sync+async' if require_async else 'sync'
    print(
        'provider contract evidence: '
        f'collected={collected} executed={collected} skipped=0 '
        f'real_asyncshotgun={real_async_shotgun}'
    )
    print(f'provider contract PASS: {provider_name} {version} {modes}')


if __name__ == '__main__':
    _main()
