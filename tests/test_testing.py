from __future__ import annotations

import asyncio
import copy
import functools
import gc
import inspect
import multiprocessing
import warnings
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

import pytest

import sg_event_daemon.testing as testing_module
from sg_event_daemon._providers import AsyncSG, SyncSG
from sg_event_daemon.testing import (
    DriveResult,
    HarnessHandle,
    HarnessSettings,
    PluginError,
    drive_async_plugin,
    drive_sync_plugin,
    load_plugin,
)

if TYPE_CHECKING:
    from collections.abc import Coroutine
    from typing import Literal


class _AsyncCallableClose:
    def __init__(self) -> None:
        self.called = False

    async def __call__(self) -> None:
        self.called = True


class _AsyncCallableCloseHandle:
    def __init__(self) -> None:
        self.close = _AsyncCallableClose()

    def set_session_uuid(self, _session_uuid: str | None) -> None:
        pass


class _AsyncCloseSyncHandle:
    def __init__(self) -> None:
        self._closed = False

    async def close(self) -> None:
        self._closed = True

    def set_session_uuid(self, _session_uuid: str | None) -> None:
        pass


class _AsyncHandle:
    def __init__(self) -> None:
        self.close_error: RuntimeError | None = None
        self._closed = False
        self.session_uuids: list[str | None] = []

    async def close(self) -> None:
        await asyncio.sleep(0)
        self._closed = True
        if self.close_error is not None:
            raise self.close_error

    async def find(
        self,
        _entity_type: str,
        _filters: object,
        _fields: list[str] | None = None,
        **_kwargs: object,
    ) -> list[dict[str, object]]:
        raise AssertionError('the harness made an unexpected SG request')

    def set_session_uuid(self, session_uuid: str | None) -> None:
        self.session_uuids.append(session_uuid)


class _AsyncSessionSyncHandle:
    def close(self) -> None:
        pass

    async def set_session_uuid(self, _session_uuid: str | None) -> None:
        pass


class _DecoratedAsyncCloseHandle:
    def __init__(self) -> None:
        self.closed = False

    async def _close_async(self) -> None:
        self.closed = True

    @functools.wraps(_close_async)
    def close(self) -> Coroutine[object, object, None]:
        return self._close_async()

    def set_session_uuid(self, _session_uuid: str | None) -> None:
        pass


class _OfflineHandle(Protocol):
    state: list[str]


class _SyncCloseAsyncHandle:
    def __init__(self) -> None:
        self._closed = False

    def close(self) -> None:
        self._closed = True

    def set_session_uuid(self, _session_uuid: str | None) -> None:
        pass


class _SyncHandle:
    def __init__(self) -> None:
        self.close_error: RuntimeError | None = None
        self._closed = False
        self._session_uuids: list[str | None] = []

    def close(self) -> None:
        self._closed = True
        if self.close_error is not None:
            raise self.close_error

    def find(
        self,
        _entity_type: str,
        _filters: object,
        _fields: list[str] | None = None,
        **_kwargs: object,
    ) -> list[dict[str, object]]:
        raise AssertionError('the harness made an unexpected SG request')

    def set_session_uuid(self, session_uuid: str | None) -> None:
        self._session_uuids.append(session_uuid)


def _event(
    event_id: int,
    *,
    event_type: str = 'Shotgun_Shot_Change',
    session_uuid: str | None = None,
) -> dict[str, object]:
    return {
        'attribute_name': 'sg_status_list',
        'created_at': '2026-07-29T12:00:00Z',
        'entity': {'id': 7, 'type': 'Shot'},
        'event_type': event_type,
        'id': event_id,
        'meta': {},
        'project': None,
        'session_uuid': session_uuid,
        'type': 'EventLogEntry',
        'user': None,
    }


def _offline_handle(class_name: str) -> _OfflineHandle:
    handle_type = cast('type[_OfflineHandle]', vars(testing_module)[class_name])
    return handle_type()


def _write_plugin(tmp_path: Path, source: str) -> Path:
    path = tmp_path / 'plugin.py'
    path.write_text(source, encoding='utf-8')
    return path


def test_async_drive_accepts_async_callable_close_handle(tmp_path: Path) -> None:
    path = _write_plugin(
        tmp_path,
        """
seen = []

async def setup(context):
    seen.append('setup')

def register(registrar):
    registrar.set_setup(setup)
""",
    )
    loaded = load_plugin(path, plugin_id='async-callable-close', execution='async')
    handle = _AsyncCallableCloseHandle()

    result = asyncio.run(drive_async_plugin(loaded, (), sg=handle))

    assert result.ok
    assert loaded.module.seen == ['setup']
    assert handle.close.called


def test_async_drive_accepts_decorated_async_close_handle(tmp_path: Path) -> None:
    path = _write_plugin(
        tmp_path,
        """
seen = []

async def setup(context):
    seen.append('setup')

def register(registrar):
    registrar.set_setup(setup)
""",
    )
    loaded = load_plugin(path, plugin_id='decorated-async-close', execution='async')
    handle = _DecoratedAsyncCloseHandle()

    result = asyncio.run(drive_async_plugin(loaded, (), sg=cast('HarnessHandle', handle)))

    assert result.ok
    assert loaded.module.seen == ['setup']
    assert handle.closed


def test_async_drive_rejects_sync_close_handle(tmp_path: Path) -> None:
    path = _write_plugin(
        tmp_path,
        """
seen = []

async def setup(context):
    seen.append('setup')

def register(registrar):
    registrar.set_setup(setup)
""",
    )
    loaded = load_plugin(path, plugin_id='async-close-mismatch', execution='async')
    handle = _SyncCloseAsyncHandle()

    with pytest.raises(ValueError, match='async handle close must be async'):
        asyncio.run(drive_async_plugin(loaded, (), sg=handle))

    assert loaded.module.seen == []
    assert not handle._closed


def test_async_harness_awaits_setup_and_callback_with_injected_handle(
    tmp_path: Path,
) -> None:
    path = _write_plugin(
        tmp_path,
        """
seen = []
seen_handles = []

async def setup(context):
    seen.append(('setup', context.plugin_id))

async def callback(context):
    seen_handles.append(context.sg)
    seen.append(
        (
            'callback',
            context.event['id'],
            context.sg.session_uuids[-1],
        )
    )

def register(registrar):
    registrar.set_setup(setup)
    registrar.add_callback(
        id='task-change',
        callback=callback,
        events={'Shotgun_Task_Change': None},
    )
""",
    )
    loaded = load_plugin(path, plugin_id='async-example', execution='async')
    handle = _AsyncHandle()

    result = asyncio.run(
        drive_async_plugin(
            loaded,
            (
                _event(41),
                _event(
                    42,
                    event_type='Shotgun_Task_Change',
                    session_uuid='async-session',
                ),
            ),
            sg=handle,
        )
    )

    assert result.ok
    assert [(callback.event_id, callback.callback_id) for callback in result.callbacks] == [
        (42, 'task-change')
    ]
    assert loaded.module.seen == [
        ('setup', 'async-example'),
        ('callback', 42, 'async-session'),
    ]
    assert len(loaded.module.seen_handles) == 1
    assert loaded.module.seen_handles[0] is handle
    assert handle.session_uuids == ['async-session']
    assert handle._closed


def test_async_harness_default_handle_guides_unsupported_operations(
    tmp_path: Path,
) -> None:
    path = _write_plugin(
        tmp_path,
        """
outcomes = []

async def callback(context):
    for operation in ('find', 'update', 'arbitrary_operation'):
        try:
            method = getattr(context.sg, operation)
            await method('Shot', [], [])
        except Exception as error:
            outcomes.append((operation, type(error).__name__, str(error)))

def register(registrar):
    registrar.add_callback(id='callback', callback=callback)
""",
    )
    loaded = load_plugin(path, plugin_id='async-offline', execution='async')

    result = asyncio.run(drive_async_plugin(loaded, (_event(42, session_uuid='browser-session'),)))

    assert result.ok
    assert loaded.module.outcomes == [
        (
            operation,
            'RuntimeError',
            'inject sg= to make SG requests from the plugin harness',
        )
        for operation in ('find', 'update', 'arbitrary_operation')
    ]


def test_async_harness_returns_redacted_callback_failure_and_stops_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensitive_value = 'async-callback-private-value'
    monkeypatch.setenv('ASYNC_CALLBACK_SECRET', sensitive_value)
    path = _write_plugin(
        tmp_path,
        """
import asyncio

seen = []

async def fail(context):
    await asyncio.sleep(0)
    seen.append(context.event['id'])
    raise RuntimeError('failure: ' + context.secrets['token'].reveal())

async def later(context):
    seen.append('later')

def register(registrar):
    registrar.add_callback(id='fail', callback=fail)
    registrar.add_callback(id='later', callback=later)
""",
    )
    loaded = load_plugin(
        path,
        plugin_id='async-failing',
        execution='async',
        settings=HarnessSettings(secrets={'token': 'ASYNC_CALLBACK_SECRET'}),
    )
    handle = _AsyncHandle()

    result = asyncio.run(drive_async_plugin(loaded, (_event(42), _event(43)), sg=handle))

    assert not result.ok
    assert result.setup_failure is None
    assert len(result.callbacks) == 1
    callback = result.callbacks[0]
    assert callback.event_id == 42
    failure = callback.failure
    assert failure is not None
    assert failure.code == 'callback-failed'
    assert (failure.site, failure.group) == ('testing', 'testing')
    assert (failure.plugin_id, failure.callback_id) == ('async-failing', 'fail')
    assert sensitive_value not in repr(result)
    assert '<redacted>' in failure.detail
    assert loaded.module.seen == [42]
    assert handle._closed


def test_complete_documented_synthetic_event_delivers() -> None:
    loaded = load_plugin(
        Path(__file__).parent / 'fixtures' / 'noop_plugin.py',
        plugin_id='fixture',
    )
    synthetic_event = {
        'id': 42,
        'type': 'EventLogEntry',
        'event_type': 'Shotgun_Shot_Change',
        'attribute_name': 'sg_status_list',
        'meta': {},
        'entity': {'id': 123, 'type': 'Shot'},
        'user': None,
        'project': {'id': 1, 'type': 'Project'},
        'session_uuid': None,
        'created_at': '2026-08-24T00:00:00+00:00',
    }

    result = drive_sync_plugin(loaded, (synthetic_event,))

    assert result.ok
    assert [(callback.callback_id, callback.failure) for callback in result.callbacks] == [
        ('noop', None)
    ]


def test_drive_helpers_reject_loaded_plugin_mode_mismatch(tmp_path: Path) -> None:
    sync_path = _write_plugin(
        tmp_path,
        """
def callback(_context):
    pass

def register(registrar):
    registrar.add_callback(id='callback', callback=callback)
""",
    )
    loaded = load_plugin(sync_path, plugin_id='sync-example')

    with pytest.raises(ValueError, match='drive_async_plugin requires an async plugin'):
        asyncio.run(drive_async_plugin(loaded, ()))


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_harness_closes_injected_handle_after_empty_run(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    prefix = 'async ' if execution == 'async' else ''
    path = _write_plugin(
        tmp_path,
        f"""
seen = []

{prefix}def setup(context):
    seen.append('setup')

{prefix}def callback(context):
    seen.append('callback')

def register(registrar):
    registrar.set_setup(setup)
    registrar.add_callback(id='callback', callback=callback)
""",
    )
    loaded = load_plugin(path, plugin_id='empty-run', execution=execution)
    handle = _AsyncHandle() if execution == 'async' else _SyncHandle()

    result = (
        asyncio.run(drive_async_plugin(loaded, (), sg=handle))
        if execution == 'async'
        else drive_sync_plugin(loaded, (), sg=handle)
    )

    assert result.ok
    assert result.setup_failure is None
    assert result.callbacks == ()
    assert loaded.module.seen == ['setup']
    assert handle._closed


def test_harness_exports_its_catchable_error() -> None:
    assert 'PluginError' in testing_module.__all__
    assert testing_module.PluginError is PluginError


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_harness_returns_redacted_setup_failure_and_closes_handle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    execution: Literal['sync', 'async'],
) -> None:
    sensitive_value = 'setup-private-value'
    monkeypatch.setenv('SETUP_SECRET', sensitive_value)
    prefix = 'async ' if execution == 'async' else ''
    path = _write_plugin(
        tmp_path,
        f"""
seen = []

{prefix}def setup(context):
    seen.append('setup')
    raise RuntimeError('setup: ' + context.secrets['token'].reveal())

{prefix}def callback(context):
    seen.append(context.event['id'])

def register(registrar):
    registrar.set_setup(setup)
    registrar.add_callback(id='callback', callback=callback)
""",
    )
    loaded = load_plugin(
        path,
        plugin_id='setup-failing',
        execution=execution,
        settings=HarnessSettings(secrets={'token': 'SETUP_SECRET'}),
    )
    handle = _AsyncHandle() if execution == 'async' else _SyncHandle()
    events = (_event(42), _event(43))

    result = (
        asyncio.run(drive_async_plugin(loaded, events, sg=handle))
        if execution == 'async'
        else drive_sync_plugin(loaded, events, sg=handle)
    )

    assert not result.ok
    assert result.callbacks == ()
    failure = result.setup_failure
    assert failure is not None
    assert failure.code == 'setup-failed'
    assert (failure.site, failure.group) == ('testing', 'testing')
    assert (failure.plugin_id, failure.callback_id) == ('setup-failing', None)
    assert sensitive_value not in repr(result)
    assert '<redacted>' in failure.detail
    assert loaded.module.seen == ['setup']
    if execution == 'async':
        assert isinstance(handle, _AsyncHandle)
        assert handle.session_uuids == []
    else:
        assert isinstance(handle, _SyncHandle)
        assert handle._session_uuids == []
    assert handle._closed


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_harness_rejects_matching_invalid_id_before_callback(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    prefix = 'async ' if execution == 'async' else ''
    path = _write_plugin(
        tmp_path,
        f"""
seen = []

{prefix}def callback(context):
    seen.append(context.event['id'])

def register(registrar):
    registrar.add_callback(id='callback', callback=callback)
""",
    )
    loaded = load_plugin(path, plugin_id='invalid-id', execution=execution)
    handle = _AsyncHandle() if execution == 'async' else _SyncHandle()

    def drive() -> None:
        if execution == 'async':
            asyncio.run(drive_async_plugin(loaded, (_event(0),), sg=handle))
        else:
            drive_sync_plugin(loaded, (_event(0),), sg=handle)

    with pytest.raises(ValueError, match='synthetic event id must be a positive integer'):
        drive()

    assert loaded.module.seen == []
    assert handle._closed


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_harness_preserves_invalid_event_error_when_teardown_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    execution: Literal['sync', 'async'],
) -> None:
    sensitive_value = 'invalid-event-teardown-private-value'
    monkeypatch.setenv('TEARDOWN_SECRET', sensitive_value)
    prefix = 'async ' if execution == 'async' else ''
    path = _write_plugin(
        tmp_path,
        f"""
{prefix}def setup(context):
    pass

{prefix}def callback(context):
    pass

def register(registrar):
    registrar.set_setup(setup)
    registrar.add_callback(id='callback', callback=callback)
""",
    )
    loaded = load_plugin(
        path,
        plugin_id='invalid-and-teardown',
        execution=execution,
        settings=HarnessSettings(secrets={'token': 'TEARDOWN_SECRET'}),
    )
    handle = _AsyncHandle() if execution == 'async' else _SyncHandle()
    handle.close_error = RuntimeError('close: ' + sensitive_value)
    invalid = _event(42)
    del invalid['type']

    def drive() -> None:
        if execution == 'async':
            asyncio.run(drive_async_plugin(loaded, (invalid,), sg=handle))
        else:
            drive_sync_plugin(loaded, (invalid,), sg=handle)

    with pytest.raises(ValueError, match='event has missing required fields: type') as raised:
        drive()

    assert handle._closed
    assert any('teardown-failed' in note for note in raised.value.__notes__)
    assert sensitive_value not in repr(raised.value.__notes__)


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_drive_inside_except_block_raises_close_failure(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
) -> None:
    prefix = 'async ' if execution == 'async' else ''
    path = _write_plugin(
        tmp_path,
        f"""
{prefix}def callback(context):
    pass

def register(registrar):
    registrar.add_callback(id='callback', callback=callback)
""",
    )
    loaded = load_plugin(path, plugin_id='ambient-error', execution=execution)
    handle = _AsyncHandle() if execution == 'async' else _SyncHandle()
    handle.close_error = RuntimeError('close failed')

    def drive() -> None:
        if execution == 'async':
            asyncio.run(drive_async_plugin(loaded, (), sg=handle))
        else:
            drive_sync_plugin(loaded, (), sg=handle)

    def fail() -> None:
        raise RuntimeError('caller error')

    ambient_error: RuntimeError | None = None
    close_failure: PluginError | None = None
    try:
        fail()
    except RuntimeError as caller_error:
        with pytest.raises(PluginError) as raised:
            drive()
        ambient_error = caller_error
        close_failure = raised.value

    assert ambient_error is not None
    assert close_failure is not None
    assert not hasattr(ambient_error, '__notes__')
    assert close_failure.code == 'teardown-failed'
    assert close_failure.__cause__ is handle.close_error
    assert handle._closed


def test_harness_settings_defaults_are_empty_and_immutable() -> None:
    settings = HarnessSettings()

    assert settings.config == {}
    assert settings.secrets == {}
    with pytest.raises(TypeError):
        cast('dict[str, object]', settings.config)['new'] = 'value'
    with pytest.raises(TypeError):
        cast('dict[str, str]', settings.secrets)['new'] = 'variable'


@pytest.mark.parametrize('execution', ['sync', 'async'])
@pytest.mark.parametrize('phase', ['empty', 'setup', 'callback'])
def test_harness_teardown_failure_propagates_redacted_plugin_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    execution: Literal['sync', 'async'],
    phase: str,
) -> None:
    sensitive_value = 'teardown-private-value'
    monkeypatch.setenv('TEARDOWN_SECRET', sensitive_value)
    prefix = 'async ' if execution == 'async' else ''
    path = _write_plugin(
        tmp_path,
        f"""
seen = []

{prefix}def setup(context):
    seen.append('setup')
    if context.config['phase'] == 'setup':
        raise RuntimeError('setup: ' + context.secrets['token'].reveal())

{prefix}def callback(context):
    seen.append(context.event['id'])
    raise RuntimeError('callback: ' + context.secrets['token'].reveal())

def register(registrar):
    registrar.set_setup(setup)
    registrar.add_callback(id='callback', callback=callback)
""",
    )
    loaded = load_plugin(
        path,
        plugin_id='teardown-failing',
        execution=execution,
        settings=HarnessSettings(
            config={'phase': phase},
            secrets={'token': 'TEARDOWN_SECRET'},
        ),
    )
    handle = _AsyncHandle() if execution == 'async' else _SyncHandle()
    handle.close_error = RuntimeError('close: ' + sensitive_value)
    events = () if phase == 'empty' else (_event(42), _event(43))

    def drive() -> None:
        if execution == 'async':
            asyncio.run(drive_async_plugin(loaded, events, sg=handle))
        else:
            drive_sync_plugin(loaded, events, sg=handle)

    with pytest.raises(PluginError) as raised:
        drive()

    failure = raised.value
    assert failure.code == 'teardown-failed'
    assert (failure.site, failure.group) == ('testing', 'testing')
    assert (failure.plugin_id, failure.callback_id) == ('teardown-failing', None)
    assert failure.__cause__ is handle.close_error
    assert sensitive_value not in str(failure)
    assert '<redacted>' in str(failure)
    assert loaded.module.seen == (['setup', 42] if phase == 'callback' else ['setup'])
    assert handle._closed


def test_load_plugin_redacts_direct_import_failure_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protected_value = 'direct-harness-import-private-value'
    monkeypatch.setenv('DIRECT_HARNESS_SECRET', protected_value)
    path = _write_plugin(
        tmp_path,
        """
import os

raise RuntimeError('harness import exposed ' + os.environ['DIRECT_HARNESS_SECRET'])
""",
    )

    with pytest.raises(PluginError) as raised:
        load_plugin(
            path,
            plugin_id='direct-import-failure',
            settings=HarnessSettings(secrets={'token': 'DIRECT_HARNESS_SECRET'}),
        )

    assert raised.value.code == 'import-failed'
    assert protected_value not in str(raised.value)
    assert '<redacted>' in str(raised.value)


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_load_plugin_redacts_real_registration_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    execution: Literal['sync', 'async'],
) -> None:
    sensitive_value = 'registration-private-value'
    monkeypatch.setenv('REGISTRATION_SECRET', sensitive_value)
    path = _write_plugin(
        tmp_path,
        """
import os

def register(registrar):
    raise RuntimeError('registration: ' + os.environ['REGISTRATION_SECRET'])
""",
    )

    with pytest.raises(PluginError) as raised:
        load_plugin(
            path,
            plugin_id='registration-failing',
            execution=execution,
            settings=HarnessSettings(secrets={'token': 'REGISTRATION_SECRET'}),
        )

    failure = raised.value
    assert failure.code == 'registration-failed'
    assert (failure.site, failure.group) == ('testing', 'testing')
    assert (failure.plugin_id, failure.callback_id) == ('registration-failing', None)
    assert sensitive_value not in str(failure)
    assert '<redacted>' in str(failure)


def test_load_plugin_reports_real_registration_and_mode_errors(tmp_path: Path) -> None:
    duplicate = _write_plugin(
        tmp_path,
        """
def callback(_context):
    pass

def register(registrar):
    registrar.add_callback(id='same', callback=callback)
    registrar.add_callback(id='same', callback=callback)
""",
    )

    with pytest.raises(PluginError, match=r'duplicate callback id.*same'):
        load_plugin(duplicate, plugin_id='duplicate')

    async_callback = _write_plugin(
        tmp_path,
        """
async def callback(_context):
    pass

def register(registrar):
    registrar.add_callback(id='async-callback', callback=callback)
""",
    )

    with pytest.raises(PluginError, match=r"sync group rejects async callback 'async-callback'"):
        load_plugin(async_callback, plugin_id='wrong-mode', execution='sync')


@pytest.mark.parametrize(
    ('class_name', 'protocol'),
    [
        ('_OfflineAsyncHandle', AsyncSG),
        ('_OfflineSyncHandle', SyncSG),
    ],
)
def test_offline_handles_preserve_introspection_protocols(
    class_name: str,
    protocol: type[AsyncSG | SyncSG],
) -> None:
    handle = _offline_handle(class_name)
    introspection_target = cast('Any', handle)

    assert isinstance(cast('object', handle), protocol)
    assert not hasattr(introspection_target, '__wrapped__')
    assert inspect.unwrap(introspection_target) is handle
    with pytest.raises(TypeError):
        inspect.get_annotations(introspection_target)


@pytest.mark.parametrize('class_name', ['_OfflineAsyncHandle', '_OfflineSyncHandle'])
def test_offline_handles_preserve_missing_private_attributes(class_name: str) -> None:
    handle = _offline_handle(class_name)
    attribute_name = '_private_operation'

    with pytest.raises(AttributeError) as caught:
        getattr(handle, attribute_name)

    assert caught.value.name == attribute_name
    assert caught.value.obj is handle


@pytest.mark.parametrize('round_trip', ['deepcopy', 'pickle'])
@pytest.mark.parametrize('class_name', ['_OfflineAsyncHandle', '_OfflineSyncHandle'])
def test_offline_handles_preserve_state_across_protocol_round_trips(
    class_name: str,
    round_trip: str,
) -> None:
    handle = _offline_handle(class_name)
    state = ['offline-state']
    handle.state = state
    restored: _OfflineHandle

    if round_trip == 'deepcopy':
        restored = copy.deepcopy(handle)
    else:
        receiver, sender = multiprocessing.Pipe(duplex=False)
        try:
            sender.send(handle)
            restored = cast('_OfflineHandle', receiver.recv())
        finally:
            receiver.close()
            sender.close()

    assert type(restored) is type(handle)
    assert restored.state == ['offline-state']
    assert restored.state is not state


def test_sync_drive_rejects_async_close_handle(tmp_path: Path) -> None:
    path = _write_plugin(
        tmp_path,
        """
seen = []

def setup(context):
    seen.append('setup')

def register(registrar):
    registrar.set_setup(setup)
""",
    )
    loaded = load_plugin(path, plugin_id='sync-close-mismatch')
    handle = _AsyncCloseSyncHandle()

    with pytest.raises(ValueError, match='sync handle close must be sync'):
        drive_sync_plugin(loaded, (), sg=handle)

    assert loaded.module.seen == []
    assert not handle._closed


def test_sync_drive_rejects_async_session_handle(tmp_path: Path) -> None:
    path = _write_plugin(
        tmp_path,
        """
seen = []

def setup(context):
    seen.append('setup')

def register(registrar):
    registrar.set_setup(setup)
""",
    )
    loaded = load_plugin(path, plugin_id='sync-session-mismatch')

    with pytest.raises(ValueError, match='sync handle set_session_uuid must be sync'):
        drive_sync_plugin(loaded, (), sg=_AsyncSessionSyncHandle())

    assert loaded.module.seen == []


def test_sync_drive_rejects_decorated_async_close_handle(tmp_path: Path) -> None:
    path = _write_plugin(
        tmp_path,
        """
seen = []

def setup(context):
    seen.append('setup')

def register(registrar):
    registrar.set_setup(setup)
""",
    )
    loaded = load_plugin(path, plugin_id='decorated-sync-close')
    handle = _DecoratedAsyncCloseHandle()

    with pytest.raises(ValueError, match='sync handle close must be sync'):
        drive_sync_plugin(loaded, (), sg=cast('HarnessHandle', handle))

    assert loaded.module.seen == []
    assert not handle.closed


def test_sync_harness_default_handle_guides_unsupported_operations(
    tmp_path: Path,
) -> None:
    path = _write_plugin(
        tmp_path,
        """
outcomes = []

def callback(context):
    for operation in ('find', 'update', 'arbitrary_operation'):
        try:
            method = getattr(context.sg, operation)
            method('Shot', [], [])
        except Exception as error:
            outcomes.append((operation, type(error).__name__, str(error)))

def register(registrar):
    registrar.add_callback(id='callback', callback=callback)
""",
    )
    loaded = load_plugin(path, plugin_id='sync-offline')

    result = drive_sync_plugin(
        loaded,
        (_event(42, session_uuid='browser-session'),),
    )

    assert result.ok
    assert loaded.module.outcomes == [
        (
            operation,
            'RuntimeError',
            'inject sg= to make SG requests from the plugin harness',
        )
        for operation in ('find', 'update', 'arbitrary_operation')
    ]


def test_sync_harness_rejects_and_closes_returned_coroutine(tmp_path: Path) -> None:
    path = _write_plugin(
        tmp_path,
        """
seen = []
returned = []

async def work():
    seen.append('executed')

def callback(context):
    seen.append(context.event['id'])
    result = work()
    returned.append(result)
    return result

def register(registrar):
    registrar.add_callback(id='returned-coroutine', callback=callback)
""",
    )
    loaded = load_plugin(path, plugin_id='sync-coroutine')
    handle = _SyncHandle()

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter('always')
        result = drive_sync_plugin(loaded, (_event(42), _event(43)), sg=handle)
        coroutines = cast('list[Coroutine[object, object, None]]', loaded.module.returned)
        states = [inspect.getcoroutinestate(coroutine) for coroutine in coroutines]
        # Close any baseline leak so a failing regression does not emit later warnings.
        for coroutine in coroutines:
            coroutine.close()
        coroutines.clear()
        gc.collect()

    assert not result.ok
    assert result.setup_failure is None
    assert len(result.callbacks) == 1
    callback = result.callbacks[0]
    assert (callback.event_id, callback.callback_id) == (42, 'returned-coroutine')
    assert callback.failure is not None
    assert callback.failure.code == 'callback-failed'
    assert "sync callback 'returned-coroutine' returned an awaitable" in callback.failure.detail
    assert loaded.module.seen == [42]
    assert states == [inspect.CORO_CLOSED]
    assert captured == []
    assert handle._closed


def test_sync_harness_returns_redacted_callback_failure_and_stops_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sensitive_value = 'failure-secret'
    monkeypatch.setenv('FAILURE_SECRET', sensitive_value)
    path = _write_plugin(
        tmp_path,
        """
seen = []

def fail(context):
    seen.append('fail')
    raise RuntimeError('failure: ' + context.secrets['token'].reveal())

def later(_context):
    seen.append('later')

def register(registrar):
    registrar.add_callback(id='fail', callback=fail)
    registrar.add_callback(id='later', callback=later)
""",
    )
    loaded = load_plugin(
        path,
        plugin_id='failing',
        settings=HarnessSettings(secrets={'token': 'FAILURE_SECRET'}),
    )

    handle = _SyncHandle()
    result = drive_sync_plugin(loaded, (_event(42), _event(43)), sg=handle)

    assert not result.ok
    assert result.setup_failure is None
    assert len(result.callbacks) == 1
    failure = result.callbacks[0].failure
    assert failure is not None
    assert failure.code == 'callback-failed'
    assert failure.plugin_id == 'failing'
    assert failure.callback_id == 'fail'
    assert (failure.site, failure.group) == ('testing', 'testing')
    assert sensitive_value not in failure.detail
    assert sensitive_value not in repr(result)
    assert '<redacted>' in failure.detail
    assert loaded.module.seen == ['fail']
    assert result.callbacks[0].event_id == 42
    assert handle._closed


def test_sync_harness_uses_setup_config_secrets_filters_context_and_shared_module_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    sensitive_value = 'harness-secret-value'
    monkeypatch.setenv('HARNESS_SECRET', sensitive_value)
    path = _write_plugin(
        tmp_path,
        """
seen = []
seen_handles = []

def setup(context):
    seen.append(
        (
            'setup',
            context.site,
            context.source,
            context.group,
            context.plugin_id,
            context.config['label'],
            str(context.secrets['token']),
        )
    )

def callback(context):
    seen_handles.append(context.sg)
    context.logger.warning('revealed=%s', context.secrets['token'].reveal())
    seen.append(
        (
            'callback',
            context.callback_id,
            context.event['id'],
            context.delivery_attempt,
            context.is_operator_replay,
        )
    )

def register(registrar):
    registrar.set_setup(setup)
    registrar.add_callback(
        id='shot-change',
        callback=callback,
        events={'Shotgun_Shot_Change': ('sg_status_list',)},
    )
""",
    )
    loaded = load_plugin(
        path,
        plugin_id='example',
        settings=HarnessSettings(
            site='studio',
            source='primary',
            group='editorial',
            config={'label': 'configured'},
            secrets={'token': 'HARNESS_SECRET'},
        ),
    )
    handle = _SyncHandle()

    with caplog.at_level('WARNING', logger='sg_event_daemon.plugin'):
        first = drive_sync_plugin(
            loaded,
            (
                _event(41, event_type='Shotgun_Task_Change'),
                _event(42, session_uuid='browser-session'),
            ),
            sg=handle,
            delivery_attempt=3,
            is_operator_replay=True,
        )
        second = drive_sync_plugin(loaded, (_event(43),))

    assert first.ok
    assert first.setup_failure is None
    assert [(result.event_id, result.callback_id) for result in first.callbacks] == [
        (42, 'shot-change')
    ]
    assert second.ok
    assert loaded.callbacks == ('shot-change',)
    assert loaded.config == {'label': 'configured'}
    with pytest.raises(TypeError):
        cast('dict[str, object]', loaded.config)['label'] = 'changed'
    assert loaded.module.seen == [
        ('setup', 'studio', 'primary', 'editorial', 'example', 'configured', '<redacted>'),
        ('callback', 'shot-change', 42, 3, True),
        ('setup', 'studio', 'primary', 'editorial', 'example', 'configured', '<redacted>'),
        ('callback', 'shot-change', 43, 1, False),
    ]
    assert len(loaded.module.seen_handles) == 2
    assert loaded.module.seen_handles[0] is handle
    assert handle._session_uuids == ['browser-session']
    assert handle._closed
    assert sensitive_value not in caplog.text
    assert '<redacted>' in caplog.text
    assert {child.name for child in tmp_path.iterdir()} - {'__pycache__'} == {'plugin.py'}

    with pytest.raises(FrozenInstanceError):
        type(first).__setattr__(first, 'setup_failure', None)
    assert isinstance(first, DriveResult)
