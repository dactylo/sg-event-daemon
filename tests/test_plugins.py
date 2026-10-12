from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import sys
import threading
import warnings
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from decimal import Decimal
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

import pytest

import sg_event_daemon._logging as logging_module
import sg_event_daemon._plugins as plugins_module
from sg_event_daemon._config import (
    CredentialConfig,
    GroupConfig,
    PluginConfig,
    SecretRef,
    SecretValue,
    SiteConfig,
)
from sg_event_daemon._events import Event
from sg_event_daemon._logging import _TextFormatter, configure_logging
from sg_event_daemon._plugins import (
    CallbackIdentity,
    CallbackManifest,
    EventContext,
    PluginError,
    PluginHost,
    PluginSpec,
    SetupContext,
    load_plugins,
    validate_sync_callback_result,
    with_heartbeat,
)
from sg_event_daemon._providers import AsyncSG, ProviderCapabilities, ProviderInterface, SyncSG

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Mapping
    from pathlib import Path
    from types import ModuleType
    from typing import Literal

_SITE_URL = 'https://studio.example.invalid'


class _FakeAsyncHandle:
    def __init__(
        self,
        close_attempts: list[int],
        ordinal: int,
    ) -> None:
        self._close_attempts = close_attempts
        self._close_error: BaseException | None = None
        self._closed = False
        self._ordinal = ordinal
        self._session_error: RuntimeError | None = None
        self._session_uuids: list[str | None] = []

    async def close(self) -> None:
        self._close_attempts.append(self._ordinal)
        if self._close_error is not None:
            raise self._close_error
        self._closed = True

    async def find(
        self,
        _entity_type: str,
        _filters: object,
        _fields: list[str] | None = None,
        *,
        limit: int = 0,
        order: list[dict[str, str]] | None = None,
        page: int = 0,
    ) -> list[dict[str, object]]:
        assert limit >= 0
        assert page >= 0
        assert order is None or isinstance(order, list)
        return []

    def set_session_uuid(self, session_uuid: str | None) -> None:
        if self._session_error is not None:
            raise self._session_error
        self._session_uuids.append(session_uuid)


class _FakeProvider:
    def __init__(self, *, async_: bool = True) -> None:
        self.name = 'fake-provider'
        self.capabilities = ProviderCapabilities(
            sync=True,
            async_=async_,
        )
        self._async_create_errors: list[BaseException] = []
        self._close_attempts: list[int] = []
        self._create_calls: list[tuple[str, str, str]] = []
        self._create_threads: list[int] = []
        self._handles: list[_FakeAsyncHandle | _FakeSyncHandle] = []

    async def create_async(
        self,
        site: str,
        credential: CredentialConfig,
    ) -> AsyncSG:
        self._create_calls.append(('async', site, credential.name))
        self._create_threads.append(threading.get_ident())
        if self._async_create_errors:
            raise self._async_create_errors.pop(0)
        handle = _FakeAsyncHandle(self._close_attempts, len(self._handles))
        self._handles.append(handle)
        return cast('AsyncSG', handle)

    def create_sync(
        self,
        site: str,
        credential: CredentialConfig,
    ) -> SyncSG:
        self._create_calls.append(('sync', site, credential.name))
        self._create_threads.append(threading.get_ident())
        handle = _FakeSyncHandle(self._close_attempts, len(self._handles))
        self._handles.append(handle)
        return cast('SyncSG', handle)


class _FakeSyncHandle:
    def __init__(
        self,
        close_attempts: list[int],
        ordinal: int,
    ) -> None:
        self._close_attempts = close_attempts
        self._close_error: BaseException | None = None
        self._closed = False
        self._ordinal = ordinal
        self._session_error: RuntimeError | None = None
        self._session_uuids: list[str | None] = []

    def close(self) -> None:
        self._close_attempts.append(self._ordinal)
        if self._close_error is not None:
            raise self._close_error
        self._closed = True

    def find(
        self,
        _entity_type: str,
        _filters: object,
        _fields: list[str] | None = None,
        *,
        limit: int = 0,
        order: list[dict[str, str]] | None = None,
        page: int = 0,
    ) -> list[dict[str, object]]:
        assert limit >= 0
        assert page >= 0
        assert order is None or isinstance(order, list)
        return []

    def set_session_uuid(self, session_uuid: str | None) -> None:
        if self._session_error is not None:
            raise self._session_error
        self._session_uuids.append(session_uuid)


class _StructuredHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self._payloads: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self._payloads.append(json.dumps(record.__dict__, default=str, sort_keys=True))


class _SynchronizedLock:
    def __init__(self, entrants: threading.Barrier) -> None:
        self._entrants = entrants
        self._local = threading.local()
        self._lock = threading.Lock()

    def __enter__(self) -> None:
        self._entrants.wait(timeout=2)
        self._lock.acquire()
        self._local._held = True

    def __exit__(
        self,
        _exception_type: object,
        _exception: object,
        _traceback: object,
    ) -> None:
        self._local._held = False
        self._lock.release()

    @property
    def _held(self) -> bool:
        return bool(getattr(self._local, '_held', False))


def _credential(name: str, script_name: str) -> CredentialConfig:
    return CredentialConfig(
        name=name,
        script_name=script_name,
        api_key=SecretValue(f'{name}-api-key'),
    )


@contextmanager
def _daemon_log_capture() -> Generator[tuple[logging.Logger, io.StringIO]]:
    daemon_logger = logging.getLogger('sg_event_daemon')
    old_handlers = list(daemon_logger.handlers)
    old_level = daemon_logger.level
    old_propagate = daemon_logger.propagate
    stream = io.StringIO()
    logger = configure_logging(stream=stream)
    try:
        yield logger, stream
    finally:
        for handler in daemon_logger.handlers:
            handler.close()
        daemon_logger.handlers[:] = old_handlers
        daemon_logger.setLevel(old_level)
        daemon_logger.propagate = old_propagate


def _event(session_uuid: str | None, *, event_type: str = 'Shotgun_Shot_Change') -> Event:
    return Event(
        {
            'attribute_name': 'sg_status_list',
            'created_at': '2026-07-26T12:00:00Z',
            'entity': {'id': 7, 'type': 'Shot'},
            'event_type': event_type,
            'id': 42,
            'meta': {},
            'project': None,
            'session_uuid': session_uuid,
            'type': 'EventLogEntry',
            'user': None,
        }
    )


def _group(
    plugins: tuple[PluginConfig, ...],
    *,
    execution: Literal['sync', 'async'] = 'sync',
) -> GroupConfig:
    return GroupConfig(
        name='editorial',
        source='events',
        plugins=plugins,
        callback_credentials='default-creds',
        execution=execution,
    )


def _host(
    site: SiteConfig,
    group: GroupConfig,
    provider: ProviderInterface,
    plugins: tuple[PluginSpec, ...],
    signals: tuple[Callable[[], None], Callable[[], bool]] | None = None,
) -> PluginHost:
    host_signals = (lambda: None, lambda: False) if signals is None else signals
    return PluginHost(
        site,
        group,
        provider,
        plugins,
        heartbeat=host_signals[0],
        shutdown_requested=host_signals[1],
    )


@pytest.fixture(autouse=True)
def _isolate_redaction_registries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(logging_module, '_REDACTION_VALUES', set[str]())
    monkeypatch.setattr(logging_module, '_ordered_redaction_values', ())


def _module(plugin: PluginSpec) -> ModuleType:
    return sys.modules[plugin.module_name]


def _plugin(
    path: Path,
    plugin_id: str,
    *,
    config: Mapping[str, object] | None = None,
    credentials: str = 'default-creds',
    secrets: Mapping[str, SecretRef] | None = None,
) -> PluginConfig:
    return PluginConfig(
        id=plugin_id,
        path=path,
        credentials=credentials,
        config={} if config is None else config,
        secrets={} if secrets is None else secrets,
    )


def _plugin_spec(
    plugin_id: str,
    *,
    setup: Callable[[SetupContext], object] | None = None,
) -> PluginSpec:
    return PluginSpec(
        id=plugin_id,
        module_name=f'tests.{plugin_id}',
        credentials='default-creds',
        config=MappingProxyType({}),
        secrets=(),
        setup=setup,
        callbacks=(),
    )


def _provider(*, async_: bool = True) -> tuple[ProviderInterface, _FakeProvider]:
    fake = _FakeProvider(async_=async_)
    return fake, fake


def _site(group: GroupConfig) -> SiteConfig:
    credentials = {
        'default-creds': _credential('default-creds', 'default-script'),
        'plugin-creds': _credential('plugin-creds', 'plugin-script'),
    }
    return SiteConfig(
        name='studio',
        server=_SITE_URL,
        default_source='events',
        credentials=MappingProxyType(credentials),
        sources=MappingProxyType({}),
        groups=MappingProxyType({group.name: group}),
        workers=MappingProxyType({}),
    )


def _write_plugin(tmp_path: Path, source: str, *, name: str = 'plugin.py') -> Path:
    path = tmp_path / name
    path.write_text(source, encoding='utf-8')
    return path


def test_async_group_requires_async_provider_capability(tmp_path: Path) -> None:
    path = _write_plugin(
        tmp_path,
        """
async def callback(ctx):
    pass

def register(reg):
    reg.add_callback(id='only', callback=callback)
""",
    )
    group = _group((_plugin(path, 'async-only'),), execution='async')
    provider, _fake = _provider(async_=False)

    with pytest.raises(PluginError, match=r'provider.*does not support async'):
        load_plugins(_site(group), group, provider)


def test_async_host_awaits_setup_and_callback_with_async_handles(tmp_path: Path) -> None:
    path = _write_plugin(
        tmp_path,
        """
contexts = []
setup_complete = False

async def setup(ctx):
    global setup_complete
    await asyncio.sleep(0)
    setup_complete = True

async def callback(ctx):
    await asyncio.sleep(0)
    contexts.append(ctx)

def register(reg):
    reg.set_setup(setup)
    reg.add_callback(id='only', callback=callback)
""",
    )
    source = 'import asyncio\n' + path.read_text(encoding='utf-8')
    path.write_text(source, encoding='utf-8')
    group = _group((_plugin(path, 'async-plugin'),), execution='async')
    site = _site(group)
    provider, fake = _provider()
    plugins = load_plugins(site, group, provider)
    host = _host(site, group, provider, plugins)

    async def _run() -> None:
        assert await host.setup_async() is None
        result = await host.invoke_async(
            plugins[0],
            plugins[0].callbacks[0],
            _event('async-session'),
            delivery_attempt=2,
            is_operator_replay=True,
        )
        assert result is None
        await host.close_async()

    asyncio.run(_run())

    assert _module(plugins[0]).setup_complete is True
    contexts = cast('list[EventContext]', _module(plugins[0]).contexts)
    assert contexts[0].delivery_attempt == 2
    handle = cast('_FakeAsyncHandle', fake._handles[0])
    assert handle._session_uuids == ['async-session']
    assert handle._closed is True


def test_async_provider_creation_cancelled_error_returns_retryable_setup_failure() -> None:
    group = _group((), execution='async')
    site = _site(group)
    provider, fake = _provider()
    protected_value = 'default-creds-api-key'
    fake._async_create_errors.append(asyncio.CancelledError(f'provider exposed {protected_value}'))
    host = _host(
        site,
        group,
        provider,
        (_plugin_spec('cancelled-provider'),),
    )

    async def _run() -> tuple[PluginError | None, PluginError | None]:
        first = await host.setup_async()
        second = await host.setup_async()
        await host.close_async()
        return first, second

    first, second = asyncio.run(_run())

    assert isinstance(first, PluginError)
    assert first.code == 'setup-failed'
    assert first.plugin_id == 'cancelled-provider'
    assert protected_value not in str(first)
    assert second is None
    assert fake._create_calls == [
        ('async', _SITE_URL, 'default-creds'),
        ('async', _SITE_URL, 'default-creds'),
    ]
    assert len(fake._handles) == 1


def test_async_session_setter_failure_returns_redacted_group_halt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protected_value = 'async-setter-private-value'
    protected_digest = hashlib.sha256(protected_value.encode()).hexdigest()
    monkeypatch.setenv('SETTER_PRIVATE_VALUE', protected_value)
    path = _write_plugin(
        tmp_path,
        """
called = False

async def callback(ctx):
    global called
    called = True

def register(reg):
    reg.add_callback(id='only', callback=callback)
""",
    )
    group = _group(
        (
            _plugin(
                path,
                'setter-failure',
                secrets={'private': SecretRef(env='SETTER_PRIVATE_VALUE')},
            ),
        ),
        execution='async',
    )
    site = _site(group)
    provider, fake = _provider()
    plugins = load_plugins(site, group, provider)
    host = _host(site, group, provider, plugins)

    async def _invoke() -> PluginError | None:
        assert await host.setup_async() is None
        handle = cast('_FakeAsyncHandle', fake._handles[0])
        handle._session_error = RuntimeError(
            f'setter exposed {protected_value} {protected_digest}'
        )
        return await host.invoke_async(
            plugins[0],
            plugins[0].callbacks[0],
            _event('browser-session'),
            delivery_attempt=1,
            is_operator_replay=False,
        )

    failure = asyncio.run(_invoke())

    assert isinstance(failure, PluginError)
    assert failure.code == 'callback-failed'
    assert failure.callback_id == 'only'
    assert protected_value not in str(failure)
    assert protected_digest not in str(failure)
    assert _module(plugins[0]).called is False


def test_async_setup_cancelled_error_retries_existing_handle() -> None:
    attempts = 0
    setup_handles: list[object] = []
    protected_value = 'default-creds-api-key'

    async def setup(context: SetupContext) -> None:
        nonlocal attempts
        attempts += 1
        setup_handles.append(context.sg)
        if attempts == 1:
            raise asyncio.CancelledError(f'setup exposed {protected_value}')

    group = _group((), execution='async')
    site = _site(group)
    provider, fake = _provider()
    host = _host(
        site,
        group,
        provider,
        (_plugin_spec('cancelled-setup', setup=setup),),
    )

    async def _run() -> tuple[PluginError | None, PluginError | None]:
        first = await host.setup_async()
        second = await host.setup_async()
        await host.close_async()
        return first, second

    first, second = asyncio.run(_run())

    assert isinstance(first, PluginError)
    assert first.code == 'setup-failed'
    assert first.plugin_id == 'cancelled-setup'
    assert protected_value not in str(first)
    assert second is None
    assert attempts == 2
    assert len(fake._handles) == 1
    assert setup_handles[0] is setup_handles[1] is fake._handles[0]


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_host_close_attempts_every_handle_and_reraises_first_base_exception(
    execution: Literal['sync', 'async'],
) -> None:
    group = _group((), execution=execution)
    site = _site(group)
    provider, fake = _provider()
    plugins = tuple(_plugin_spec(f'plugin-{ordinal}') for ordinal in range(3))
    host = _host(site, group, provider, plugins)

    if execution == 'async':
        assert asyncio.run(host.setup_async()) is None
    else:
        assert host.setup_sync() is None
    first_error = KeyboardInterrupt('first close failure')
    fake._handles[2]._close_error = first_error
    fake._handles[0]._close_error = SystemExit('later close failure')

    def _close() -> None:
        if execution == 'async':
            asyncio.run(host.close_async())
        else:
            host.close_sync()

    with pytest.raises(KeyboardInterrupt) as raised:
        _close()

    assert raised.value is first_error
    assert fake._close_attempts == [2, 1, 0]

    if execution == 'async':
        assert asyncio.run(host.setup_async()) is None
        asyncio.run(host.close_async())
    else:
        assert host.setup_sync() is None
        host.close_sync()

    assert len(fake._handles) == 6
    assert fake._close_attempts == [2, 1, 0, 5, 4, 3]


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_host_close_finishes_cleanup_before_rendering_first_failure(
    execution: Literal['sync', 'async'],
) -> None:
    protected_value = 'hostile-close-private-value'

    class _HostileCloseError(RuntimeError):
        def __str__(self) -> str:
            raise ValueError(f'cannot render {protected_value}')

    group = _group((), execution=execution)
    site = _site(group)
    provider, fake = _provider()
    plugins = tuple(_plugin_spec(f'plugin-{ordinal}') for ordinal in range(3))
    host = _host(site, group, provider, plugins)

    if execution == 'async':
        assert asyncio.run(host.setup_async()) is None
    else:
        assert host.setup_sync() is None
    first_error = _HostileCloseError(protected_value)
    fake._handles[2]._close_error = first_error
    fake._handles[0]._close_error = RuntimeError('later close failure')

    def _close() -> None:
        if execution == 'async':
            asyncio.run(host.close_async())
        else:
            host.close_sync()

    with pytest.raises(PluginError) as raised:
        _close()

    assert raised.value.code == 'teardown-failed'
    assert raised.value.plugin_id == 'plugin-2'
    assert raised.value.__cause__ is first_error
    assert protected_value not in str(raised.value)
    assert '<unprintable>' in str(raised.value)
    assert fake._close_attempts == [2, 1, 0]


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_host_close_redacts_first_ordinary_failure_and_preserves_cause(
    execution: Literal['sync', 'async'],
) -> None:
    group = _group((), execution=execution)
    site = _site(group)
    provider, fake = _provider()
    plugins = tuple(_plugin_spec(f'plugin-{ordinal}') for ordinal in range(3))
    host = _host(site, group, provider, plugins)

    if execution == 'async':
        assert asyncio.run(host.setup_async()) is None
    else:
        assert host.setup_sync() is None
    protected_value = site.credentials['default-creds'].api_key.reveal()
    first_error = RuntimeError(f'first close exposed {protected_value}')
    later_error = ValueError(f'later close exposed {protected_value}')
    fake._handles[2]._close_error = first_error
    fake._handles[0]._close_error = later_error

    def _close() -> None:
        if execution == 'async':
            asyncio.run(host.close_async())
        else:
            host.close_sync()

    with pytest.raises(PluginError) as raised:
        _close()

    assert raised.value.code == 'teardown-failed'
    assert raised.value.plugin_id == 'plugin-2'
    assert raised.value.__cause__ is first_error
    assert protected_value not in str(raised.value)
    assert '<redacted>' in str(raised.value)
    assert fake._close_attempts == [2, 1, 0]


def test_hosted_logger_attaches_exact_filter_when_existing_filter_compares_equal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _EqualityCollidingFilter(logging.Filter):
        def __eq__(self, _other: object) -> bool:
            return True

        def __hash__(self) -> int:
            return 0

    secret_filter_state: list[logging.Filter] = []
    monkeypatch.setattr(plugins_module, '_SECRET_FILTER_STATE', secret_filter_state)
    monkeypatch.setattr(plugins_module, '_SECRET_FILTER_LOCK', threading.Lock())
    plugin = _plugin_spec('equality-collision-filter')
    group = _group(())
    site = _site(group)
    provider, _fake = _provider()
    host = _host(site, group, provider, (plugin,))
    hosted_logger = cast(
        'Callable[[PluginHost, PluginSpec], logging.Logger]',
        vars(type(host))['_logger'],
    )
    logger = logging.getLogger('sg_event_daemon.plugin.studio.editorial.equality-collision-filter')
    colliding_filter = _EqualityCollidingFilter()
    filters: list[logging.Filter] = [colliding_filter]
    monkeypatch.setattr(logger, 'filters', filters)

    assert hosted_logger(host, plugin) is logger
    assert filters[0] is colliding_filter
    assert sum(existing is secret_filter_state[0] for existing in filters) == 1


def test_hosted_logger_concurrent_access_attaches_secret_filter_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(plugins_module, '_SECRET_FILTER_STATE', [])
    secret_filter_factory = cast(
        'Callable[[], logging.Filter]',
        vars(plugins_module)['_secret_filter'],
    )
    secret_filter = secret_filter_factory()
    attachment_lock = _SynchronizedLock(threading.Barrier(2))
    membership_entrants = threading.Barrier(2)

    class _SynchronizedFilters(list[logging.Filter]):
        def __contains__(self, item: object) -> bool:
            contains = super().__contains__(item)
            if not attachment_lock._held:
                membership_entrants.wait(timeout=2)
            return contains

    plugin = _plugin_spec('concurrent-filter')
    group = _group(())
    site = _site(group)
    provider, _fake = _provider()
    host = _host(site, group, provider, (plugin,))
    hosted_logger = cast(
        'Callable[[PluginHost, PluginSpec], logging.Logger]',
        vars(type(host))['_logger'],
    )
    logger = logging.getLogger('sg_event_daemon.plugin.studio.editorial.concurrent-filter')
    filters = _SynchronizedFilters()
    monkeypatch.setattr(plugins_module, '_SECRET_FILTER_LOCK', attachment_lock)
    monkeypatch.setattr(logger, 'filters', filters)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(hosted_logger, host, plugin) for _index in range(2)]
        loggers = [future.result(timeout=3) for future in futures]

    assert loggers == [logger, logger]
    assert sum(item is secret_filter for item in filters) == 1


def test_hosted_logger_does_not_run_filter_equality_under_attachment_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attachment_lock = threading.Lock()
    lock_attempts: list[bool] = []

    class _LockTakingFilter(logging.Filter):
        def __eq__(self, _other: object) -> bool:
            acquired = attachment_lock.acquire(timeout=0.1)
            lock_attempts.append(acquired)
            if acquired:
                attachment_lock.release()
            return False

        def __hash__(self) -> int:
            return 0

    secret_filter_state: list[logging.Filter] = []
    monkeypatch.setattr(plugins_module, '_SECRET_FILTER_STATE', secret_filter_state)
    monkeypatch.setattr(plugins_module, '_SECRET_FILTER_LOCK', attachment_lock)
    plugin = _plugin_spec('lock-taking-filter')
    group = _group(())
    site = _site(group)
    provider, _fake = _provider()
    host = _host(site, group, provider, (plugin,))
    hosted_logger = cast(
        'Callable[[PluginHost, PluginSpec], logging.Logger]',
        vars(type(host))['_logger'],
    )
    logger = logging.getLogger('sg_event_daemon.plugin.studio.editorial.lock-taking-filter')
    filters: list[logging.Filter] = [_LockTakingFilter()]
    monkeypatch.setattr(logger, 'filters', filters)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(hosted_logger, host, plugin)
        assert future.result(timeout=1) is logger

    assert lock_attempts == []
    assert sum(existing is secret_filter_state[0] for existing in filters) == 1


def test_hosted_logger_preserves_redacted_exception_shape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protected_value = 'hosted-exception-private-value'
    monkeypatch.setenv('PLUGIN_SECRET', protected_value)
    path = _write_plugin(
        tmp_path,
        """
def raise_lookup_error(value):
    raise LookupError(f'lookup failed with {value}')

def raise_os_error(value):
    try:
        raise_lookup_error(value)
    except LookupError as error:
        raise OSError(f'request failed with {value}') from error

def raise_context_error(value):
    try:
        raise_lookup_error(value)
    except LookupError:
        raise OSError(f'request failed with {value}')

def raise_value_error(value):
    raise ValueError(f'request failed with {value}')

def setup(ctx):
    value = ctx.secrets['token'].reveal()
    for error_type, raise_error in (
        (ValueError, raise_value_error),
        (OSError, raise_os_error),
        (OSError, raise_context_error),
    ):
        try:
            raise_error(value)
        except error_type:
            ctx.logger.exception('plugin failed with %s', value)

def register(reg):
    reg.set_setup(setup)
""",
    )
    group = _group(
        (
            _plugin(
                path,
                'exception-logging',
                secrets={'token': SecretRef(env='PLUGIN_SECRET')},
            ),
        )
    )
    site = _site(group)
    provider, _fake = _provider()
    plugins = load_plugins(site, group, provider)
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(_TextFormatter())
    daemon_logger = logging.getLogger('sg_event_daemon')
    old_handlers = list(daemon_logger.handlers)
    old_level = daemon_logger.level
    old_propagate = daemon_logger.propagate
    daemon_logger.handlers[:] = [handler]
    daemon_logger.setLevel(logging.INFO)
    daemon_logger.propagate = False
    host = _host(site, group, provider, plugins)

    try:
        assert host.setup_sync() is None
    finally:
        daemon_logger.handlers[:] = old_handlers
        daemon_logger.setLevel(old_level)
        daemon_logger.propagate = old_propagate
        handler.close()

    output = stream.getvalue()
    exceptions = output
    assert 'ValueError: request failed with <redacted>' in exceptions
    assert 'LookupError: lookup failed with <redacted>' in exceptions
    assert 'OSError: request failed with <redacted>' in exceptions
    assert 'raise_value_error' in exceptions
    assert 'raise_lookup_error' in exceptions
    assert 'raise_os_error' in exceptions
    assert 'raise_context_error' in exceptions
    assert 'The above exception was the direct cause of the following exception:' in exceptions
    assert 'During handling of the above exception, another exception occurred:' in exceptions
    assert 'RuntimeError' not in exceptions
    assert 'plugin failed with <redacted>' in output
    assert protected_value not in output


@pytest.mark.parametrize('stage', ['setup', 'callback', 'teardown'])
def test_lifecycle_failure_logs_use_preloaded_redaction_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    protected_value = 'lifecycle-plugin-private-value'
    monkeypatch.setenv('LIFECYCLE_PLUGIN_SECRET', protected_value)
    path = _write_plugin(
        tmp_path,
        """
import os

STAGE = 'FAILURE_STAGE'

def setup(ctx):
    if STAGE == 'setup':
        raise RuntimeError(
            'setup exposed CREDENTIAL_VALUE ' + os.environ['LIFECYCLE_PLUGIN_SECRET']
        )

def callback(ctx):
    if STAGE == 'callback':
        raise RuntimeError(
            'callback exposed CREDENTIAL_VALUE ' + os.environ['LIFECYCLE_PLUGIN_SECRET']
        )

def register(reg):
    reg.set_setup(setup)
    reg.add_callback(id='only', callback=callback)
""",
    )
    group = _group(
        (
            _plugin(
                path,
                'lifecycle-failure',
                credentials='plugin-creds',
                secrets={'token': SecretRef(env='LIFECYCLE_PLUGIN_SECRET')},
            ),
        )
    )
    site = _site(group)
    credential = site.credentials['plugin-creds'].api_key.reveal()
    path.write_text(
        path.read_text(encoding='utf-8')
        .replace('CREDENTIAL_VALUE', credential)
        .replace('FAILURE_STAGE', stage),
        encoding='utf-8',
    )
    provider, fake = _provider()

    with _daemon_log_capture() as (logger, stream):
        plugins = load_plugins(site, group, provider)
        host = _host(site, group, provider, plugins)
        setup_failure = host.setup_sync()
        if stage == 'setup':
            assert isinstance(setup_failure, PluginError)
            failure: BaseException = setup_failure
        elif stage == 'callback':
            assert setup_failure is None
            callback_failure = host.invoke_sync(
                plugins[0],
                plugins[0].callbacks[0],
                _event(None),
                delivery_attempt=1,
                is_operator_replay=False,
            )
            assert isinstance(callback_failure, PluginError)
            failure = callback_failure
        else:
            assert setup_failure is None
            failure = RuntimeError(f'teardown exposed {credential} {protected_value}')
            fake._handles[0]._close_error = failure
        try:
            if stage == 'teardown':
                host.close_sync()
            else:
                raise failure
        except (PluginError, RuntimeError):
            logger.exception('%s lifecycle failure', stage)
        if stage != 'teardown':
            host.close_sync()
        output = stream.getvalue()
    assert credential not in output
    assert protected_value not in output
    assert output.count('<redacted>') >= 2


@pytest.mark.parametrize(
    ('source', 'code'),
    [
        (
            """
import os

raise RuntimeError(
    'load exposed CREDENTIAL_VALUE ' + os.environ['LOAD_PLUGIN_SECRET']
)
""",
            'import-failed',
        ),
        (
            """
import os

def register(reg):
    raise RuntimeError(
        'load exposed CREDENTIAL_VALUE ' + os.environ['LOAD_PLUGIN_SECRET']
    )
""",
            'registration-failed',
        ),
    ],
)
def test_load_failures_redact_configured_values_before_plugin_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    code: str,
) -> None:
    protected_value = 'preload-plugin-private-value'
    monkeypatch.setenv('LOAD_PLUGIN_SECRET', protected_value)
    path = _write_plugin(tmp_path, source)
    group = _group(
        (
            _plugin(
                path,
                'load-failure',
                credentials='plugin-creds',
                secrets={'token': SecretRef(env='LOAD_PLUGIN_SECRET')},
            ),
        )
    )
    site = _site(group)
    credential = site.credentials['plugin-creds'].api_key.reveal()
    path.write_text(
        path.read_text(encoding='utf-8').replace('CREDENTIAL_VALUE', credential),
        encoding='utf-8',
    )
    provider, _fake = _provider()

    with _daemon_log_capture() as (logger, stream):
        with pytest.raises(PluginError) as raised:
            load_plugins(site, group, provider)
        error = raised.value
        diagnostic = f'lane:editorial thread failed: {type(error).__name__}: {error}'
        try:
            raise error
        except PluginError:
            logger.exception('worker diagnostic: %s', diagnostic)
        output = stream.getvalue()

    surfaces = (str(error), diagnostic, output)
    assert error.code == code
    assert all(credential not in surface for surface in surfaces)
    assert all(protected_value not in surface for surface in surfaces)
    assert all('<redacted>' in surface for surface in surfaces)


def test_loader_rejects_directory_and_duplicate_plugin_ids(tmp_path: Path) -> None:
    provider, _fake = _provider()
    directory_group = _group((_plugin(tmp_path, 'directory'),))

    with pytest.raises(PluginError, match='regular file'):
        load_plugins(_site(directory_group), directory_group, provider)

    path = _write_plugin(tmp_path, 'def register(reg):\n    pass\n')
    duplicate_group = _group((_plugin(path, 'same'), _plugin(path, 'same')))
    with pytest.raises(PluginError, match=r'duplicate plugin id.*same'):
        load_plugins(_site(duplicate_group), duplicate_group, provider)


@pytest.mark.parametrize(
    ('execution', 'source', 'match'),
    [
        (
            'sync',
            """
async def setup(ctx):
    pass

def callback(ctx):
    pass

def register(reg):
    reg.set_setup(setup)
    reg.add_callback(id='only', callback=callback)
""",
            'sync group.*async setup',
        ),
        (
            'sync',
            """
async def callback(ctx):
    pass

def register(reg):
    reg.add_callback(id='only', callback=callback)
""",
            'sync group.*async callback',
        ),
        (
            'async',
            """
def setup(ctx):
    from pathlib import Path
    Path('SETUP_MARKER').write_text('ran', encoding='utf-8')

async def callback(ctx):
    pass

def register(reg):
    reg.set_setup(setup)
    reg.add_callback(id='only', callback=callback)
""",
            'async group.*ordinary setup',
        ),
        (
            'async',
            """
async def setup(ctx):
    pass

def callback(ctx):
    raise AssertionError('callback must not run')

def register(reg):
    reg.set_setup(setup)
    reg.add_callback(id='only', callback=callback)
""",
            'async group.*ordinary callback',
        ),
    ],
)
def test_loader_rejects_mode_mismatches_before_setup(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
    source: str,
    match: str,
) -> None:
    marker = tmp_path / 'setup-ran'
    path = _write_plugin(tmp_path, source.replace('SETUP_MARKER', str(marker)))
    group = _group((_plugin(path, 'mode'),), execution=execution)
    provider, _fake = _provider()

    with pytest.raises(PluginError, match=match):
        load_plugins(_site(group), group, provider)

    assert not marker.exists()


@pytest.mark.parametrize(
    ('execution', 'setup_class', 'callback_class', 'match'),
    [
        ('sync', 'SyncCallback', 'SyncCallback', None),
        ('async', 'AsyncCallback', 'AsyncCallback', None),
        ('sync', 'SyncCallback', 'AsyncCallback', 'sync group.*async callback'),
        ('async', 'AsyncCallback', 'SyncCallback', 'async group.*ordinary callback'),
    ],
)
def test_loader_classifies_callable_class_modes_before_setup(
    tmp_path: Path,
    execution: Literal['sync', 'async'],
    setup_class: str,
    callback_class: str,
    match: str | None,
) -> None:
    marker = tmp_path / 'setup-ran'
    path = _write_plugin(
        tmp_path,
        f"""
from pathlib import Path

class AsyncCallback:
    async def __call__(self, context):
        Path({str(marker)!r}).write_text('ran', encoding='utf-8')

class SyncCallback:
    def __call__(self, context):
        Path({str(marker)!r}).write_text('ran', encoding='utf-8')

def register(reg):
    reg.set_setup({setup_class}())
    reg.add_callback(id='only', callback={callback_class}())
""",
    )
    group = _group((_plugin(path, 'callable-class'),), execution=execution)
    provider, _fake = _provider()

    if match is None:
        plugins = load_plugins(_site(group), group, provider)

        assert len(plugins) == 1
    else:
        with pytest.raises(PluginError, match=match):
            load_plugins(_site(group), group, provider)

    assert not marker.exists()


def test_loader_uses_private_site_group_plugin_namespaces_for_reused_files(
    tmp_path: Path,
) -> None:
    path = _write_plugin(
        tmp_path,
        """
count = 0
seen = []

def callback(ctx):
    global count
    count += 1
    seen.append((__name__, count, ctx.plugin_id))

def register(reg):
    reg.add_callback(id='record', callback=callback)
""",
    )
    group = _group((_plugin(path, 'first'), _plugin(path, 'second')))
    site = _site(group)
    provider, _fake = _provider()

    plugins = load_plugins(site, group, provider)

    assert tuple(plugin.module_name for plugin in plugins) == (
        'sg_event_daemon._trusted_plugins.studio.editorial.first',
        'sg_event_daemon._trusted_plugins.studio.editorial.second',
    )
    assert _module(plugins[0]) is not _module(plugins[1])
    assert _module(plugins[0]).seen == []
    assert _module(plugins[1]).seen == []

    host = _host(site, group, provider, plugins)
    assert host.setup_sync() is None
    assert (
        host.invoke_sync(
            plugins[0],
            plugins[0].callbacks[0],
            _event(None),
            delivery_attempt=1,
            is_operator_replay=False,
        )
        is None
    )
    assert (
        host.invoke_sync(
            plugins[1],
            plugins[1].callbacks[0],
            _event(None),
            delivery_attempt=1,
            is_operator_replay=False,
        )
        is None
    )
    assert _module(plugins[0]).seen == [
        ('sg_event_daemon._trusted_plugins.studio.editorial.first', 1, 'first')
    ]
    assert _module(plugins[1]).seen == [
        ('sg_event_daemon._trusted_plugins.studio.editorial.second', 1, 'second')
    ]


def test_log_redaction_handles_broken_object_stringification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protected_value = 'broken-log-private-value'
    protected_digest = hashlib.sha256(protected_value.encode()).hexdigest()
    monkeypatch.setenv('PLUGIN_SECRET', protected_value)
    path = _write_plugin(
        tmp_path,
        """
import hashlib

class Broken:
    def __init__(self, value):
        self.value = value

    def __str__(self):
        raise RuntimeError(f'cannot render {self.value}')

def setup(ctx):
    value = ctx.secrets['token'].reveal()
    digest = hashlib.sha256(value.encode()).hexdigest()
    ctx.logger.warning('safe message', extra={'plugin_object': Broken(f'{value}:{digest}')})

def register(reg):
    reg.set_setup(setup)
""",
    )
    group = _group(
        (
            _plugin(
                path,
                'broken-object-logging',
                secrets={'token': SecretRef(env='PLUGIN_SECRET')},
            ),
        )
    )
    site = _site(group)
    provider, _fake = _provider()
    plugins = load_plugins(site, group, provider)
    structured_handler = _StructuredHandler()
    logger = logging.getLogger('sg_event_daemon.plugin')
    logger.addHandler(structured_handler)
    host = _host(site, group, provider, plugins)

    try:
        assert host.setup_sync() is None
    finally:
        logger.removeHandler(structured_handler)

    assert structured_handler._payloads
    assert all(protected_value not in payload for payload in structured_handler._payloads)
    assert all(protected_digest not in payload for payload in structured_handler._payloads)
    assert all('<unprintable>' in payload for payload in structured_handler._payloads)


def test_log_redaction_sanitizes_arbitrary_message_and_extra_objects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protected_value = 'object-log-private-value'
    protected_digest = hashlib.sha256(protected_value.encode()).hexdigest()
    monkeypatch.setenv('PLUGIN_SECRET', protected_value)
    path = _write_plugin(
        tmp_path,
        """
import hashlib

class Revealing:
    def __init__(self, label, value):
        self.label = label
        self.value = value

    def __str__(self):
        return f'{self.label}={self.value}'

def setup(ctx):
    value = ctx.secrets['token'].reveal()
    digest = hashlib.sha256(value.encode()).hexdigest()
    ctx.logger.warning(
        Revealing('message', f'{value}:{digest}'),
        extra={'plugin_object': Revealing('extra', f'{digest}:{value}')},
    )

def register(reg):
    reg.set_setup(setup)
""",
    )
    group = _group(
        (
            _plugin(
                path,
                'object-logging',
                secrets={'token': SecretRef(env='PLUGIN_SECRET')},
            ),
        )
    )
    site = _site(group)
    provider, _fake = _provider()
    plugins = load_plugins(site, group, provider)
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    structured_handler = _StructuredHandler()
    logger = logging.getLogger('sg_event_daemon.plugin')
    logger.addHandler(handler)
    logger.addHandler(structured_handler)
    host = _host(site, group, provider, plugins)

    try:
        assert host.setup_sync() is None
    finally:
        logger.removeHandler(handler)
        logger.removeHandler(structured_handler)

    surfaces = (stream.getvalue(), *structured_handler._payloads)
    assert all(protected_value not in surface for surface in surfaces)
    assert all(protected_digest not in surface for surface in surfaces)
    assert 'message=<redacted>:<redacted>' in stream.getvalue()
    assert any(
        'message=<redacted>:<redacted>' in payload for payload in structured_handler._payloads
    )
    assert any(
        'extra=<redacted>:<redacted>' in payload for payload in structured_handler._payloads
    )


def test_manifest_is_exact_immutable_and_uses_plugin_credential_override(
    tmp_path: Path,
) -> None:
    path = _write_plugin(
        tmp_path,
        """
def change_shot(ctx):
    pass

def register(reg):
    reg.add_callback(
        id='change-shot',
        callback=change_shot,
        events={'Shotgun_Shot_Change': ('sg_status_list',)},
    )
""",
    )
    config = {
        'labels': ['b', 'a'],
        'nested': {'answer': 42},
    }
    secrets = {
        'zeta': SecretRef(env='PLUGIN_ZETA'),
        'alpha': SecretRef(env='PLUGIN_ALPHA'),
    }
    group = _group(
        (
            _plugin(
                path,
                'manifested',
                config=config,
                credentials='plugin-creds',
                secrets=secrets,
            ),
        )
    )
    site = _site(group)
    provider, _fake = _provider()

    plugins = load_plugins(site, group, provider)
    manifest = CallbackManifest(
        callbacks=tuple(callback.identity for plugin in plugins for callback in plugin.callbacks)
    )
    expected = CallbackIdentity(
        ordinal=0,
        plugin_id='manifested',
        callback_id='change-shot',
        execution='sync',
        filter_digest=hashlib.sha256(b'[["Shotgun_Shot_Change",["sg_status_list"]]]').hexdigest(),
        config_digest=hashlib.sha256(b'{"labels":["b","a"],"nested":{"answer":42}}').hexdigest(),
        secret_refs=(('alpha', 'PLUGIN_ALPHA'), ('zeta', 'PLUGIN_ZETA')),
        credential_name='plugin-creds',
        credential_fingerprint=hashlib.sha256(
            b'{"name":"plugin-creds","script_name":"plugin-script"}'
        ).hexdigest(),
    )

    assert manifest == CallbackManifest(callbacks=(expected,))
    assert json.loads(manifest.to_json()) == {
        'callbacks': [
            {
                'callback_id': 'change-shot',
                'config_digest': expected.config_digest,
                'credential_fingerprint': expected.credential_fingerprint,
                'credential_name': 'plugin-creds',
                'execution': 'sync',
                'filter_digest': expected.filter_digest,
                'ordinal': 0,
                'plugin_id': 'manifested',
                'secret_refs': [['alpha', 'PLUGIN_ALPHA'], ['zeta', 'PLUGIN_ZETA']],
            }
        ]
    }


def test_missing_plugin_secret_fails_startup_before_handle_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('MISSING_PLUGIN_SECRET', raising=False)
    path = _write_plugin(
        tmp_path,
        """
def setup(ctx):
    raise AssertionError('setup must not run')

def register(reg):
    reg.set_setup(setup)
""",
    )
    group = _group(
        (
            _plugin(
                path,
                'missing-secret',
                secrets={'token': SecretRef(env='MISSING_PLUGIN_SECRET')},
            ),
        )
    )
    site = _site(group)
    provider, fake = _provider()
    plugins = load_plugins(site, group, provider)
    host = _host(site, group, provider, plugins)

    halt = host.setup_sync()

    assert isinstance(halt, PluginError)
    assert halt.code == 'secret-missing'
    assert 'MISSING_PLUGIN_SECRET' in str(halt)
    assert fake._create_calls == []


def test_plugin_error_redacts_overlapping_secrets_in_rendered_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    site_name = 'identity-private-value'
    group_name = f'{site_name}-group'
    monkeypatch.setenv('IDENTITY_SITE_SECRET', site_name)
    monkeypatch.setenv('IDENTITY_GROUP_SECRET', group_name)
    path = _write_plugin(tmp_path, "raise RuntimeError('identity load failed')\n")
    plugin = _plugin(
        path,
        'identity-failure',
        secrets={
            'group': SecretRef(env='IDENTITY_GROUP_SECRET'),
            'site': SecretRef(env='IDENTITY_SITE_SECRET'),
        },
    )
    group = GroupConfig(
        name=group_name,
        source='events',
        plugins=(plugin,),
        callback_credentials='default-creds',
    )
    original_site = _site(group)
    site = SiteConfig(
        name=site_name,
        server=original_site.server,
        default_source=original_site.default_source,
        credentials=original_site.credentials,
        sources=original_site.sources,
        groups=MappingProxyType({group.name: group}),
        workers=original_site.workers,
    )
    provider, _fake = _provider()

    with pytest.raises(PluginError) as raised:
        load_plugins(site, group, provider)

    assert raised.value.site == site_name
    assert raised.value.group == group_name
    assert site_name not in str(raised.value)
    assert group_name not in str(raised.value)
    assert str(raised.value).count('<redacted>') >= 2


@pytest.mark.parametrize(
    ('message', 'args'),
    [
        ('elapsed %.2f seconds for %d items', (Decimal('1.5'), 7)),
        (
            'elapsed %(duration).2f seconds for %(count)d items',
            {'count': 7, 'duration': Decimal('1.5')},
        ),
    ],
)
def test_plugin_numeric_format_specifiers_survive_redaction(
    message: str,
    args: tuple[object, ...] | Mapping[str, object],
) -> None:
    filter_type = cast(
        'type[logging.Filter]',
        vars(plugins_module)['_SecretFilter'],
    )
    record = logging.LogRecord(
        'sg_event_daemon.plugin.test',
        logging.INFO,
        __file__,
        0,
        message,
        args,
        None,
    )

    assert filter_type().filter(record)
    assert record.getMessage() == 'elapsed 1.50 seconds for 7 items'


def test_plugin_opaque_scalar_rendering_is_still_redacted() -> None:
    class _Token:
        def __str__(self) -> str:
            return 'token plugin-opaque-private-value'

    register_secret = cast(
        'Callable[[str], None]',
        vars(plugins_module)['_register_secret'],
    )
    filter_type = cast(
        'type[logging.Filter]',
        vars(plugins_module)['_SecretFilter'],
    )
    protected_value = 'plugin-opaque-private-value'
    register_secret(protected_value)
    record = logging.LogRecord(
        'sg_event_daemon.plugin.test',
        logging.INFO,
        __file__,
        0,
        'value: %s',
        (_Token(),),
        None,
    )

    assert filter_type().filter(record)
    assert record.getMessage() == 'value: token <redacted>'
    assert protected_value not in record.getMessage()


def test_plugin_redact_value_preserves_shared_acyclic_children() -> None:
    register_secret = cast(
        'Callable[[str], None]',
        vars(plugins_module)['_register_secret'],
    )
    redact_value = cast(
        'Callable[[object], object]',
        vars(plugins_module)['_redact_value'],
    )
    protected_value = 'plugin-shared-acyclic-private-value'
    register_secret(protected_value)
    shared = {'token': protected_value}

    result = redact_value({'a': shared, 'b': shared})

    assert result == {
        'a': {'token': '<redacted>'},
        'b': {'token': '<redacted>'},
    }


def test_plugin_redact_value_terminates_on_self_referential_mapping_cycle() -> None:
    redact_value = cast(
        'Callable[[object], object]',
        vars(plugins_module)['_redact_value'],
    )
    cyclic: dict[str, object] = {}
    cyclic['self'] = cyclic

    result = redact_value(cyclic)

    assert result == {'self': '<redacted-cycle>'}


def test_plugin_redaction_does_not_resort_overlapping_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    register_secret = cast(
        'Callable[[str], None]',
        vars(plugins_module)['_register_secret'],
    )
    redact_text = cast(
        'Callable[[str], str]',
        vars(plugins_module)['_redact_text'],
    )
    short_value = 'plugin-cache-order-token'
    long_value = f'{short_value}-suffix'
    short_digest = hashlib.sha256(short_value.encode()).hexdigest()
    long_digest = hashlib.sha256(long_value.encode()).hexdigest()
    register_secret(short_value)
    register_secret(long_value)
    rendered = (
        f'long={long_value} short={short_value} '
        f'long_digest={long_digest} short_digest={short_digest}'
    )
    expected = 'long=<redacted> short=<redacted> long_digest=<redacted> short_digest=<redacted>'
    sorted_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def blocked_sorted(*args: object, **kwargs: object) -> list[object]:
        sorted_calls.append((args, kwargs))
        raise AssertionError('plugin redaction called builtins.sorted')

    with monkeypatch.context() as blocked:
        blocked.setattr('builtins.sorted', blocked_sorted)
        outputs = (
            redact_text(rendered),
            redact_text(rendered),
        )

    assert outputs == (expected, expected)
    assert long_value not in ''.join(outputs)
    assert '<redacted>-suffix' not in ''.join(outputs)
    assert sorted_calls == []

    new_value = 'plugin-cache-order-fresh'
    register_secret(new_value)
    with monkeypatch.context() as blocked:
        blocked.setattr('builtins.sorted', blocked_sorted)
        refreshed = redact_text(f'refreshed={new_value}')

    assert refreshed == 'refreshed=<redacted>'
    assert sorted_calls == []


def test_plugin_redaction_preserves_existing_value_shapes() -> None:
    register_secret = cast(
        'Callable[[str], None]',
        vars(plugins_module)['_register_secret'],
    )
    redact_value = cast(
        'Callable[[object], object]',
        vars(plugins_module)['_redact_value'],
    )
    protected_value = 'plugin-shape-private-value'
    register_secret(protected_value)

    result = redact_value(
        {
            7: (
                [protected_value],
                {protected_value},
                frozenset({protected_value}),
                deque([protected_value]),
                protected_value.encode(),
                None,
                True,
                1.5,
            )
        }
    )

    assert result == {
        7: (
            ['<redacted>'],
            {'<redacted>'},
            frozenset({'<redacted>'}),
            "deque(['<redacted>'])",
            b'<redacted>',
            None,
            True,
            1.5,
        )
    }


def test_plugin_secret_filter_preserves_record_shape() -> None:
    register_secret = cast(
        'Callable[[str], None]',
        vars(plugins_module)['_register_secret'],
    )
    filter_type = cast(
        'type[logging.Filter]',
        vars(plugins_module)['_SecretFilter'],
    )
    protected_value = 'plugin-record-private-value'
    for secret in ('msg', 'args', 'name', 'levelname', protected_value):
        register_secret(secret)
    record = logging.LogRecord(
        'sg_event_daemon.plugin.test',
        logging.INFO,
        __file__,
        0,
        'value: %s',
        (protected_value,),
        None,
    )
    original_keys = set(record.__dict__)

    assert filter_type().filter(record)
    assert set(record.__dict__) == original_keys
    assert {'msg', 'args', 'name', 'levelname'} <= set(record.__dict__)
    assert protected_value not in record.getMessage()


def test_provider_validation_uses_sitewide_preloaded_redaction_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protected_value = 'provider-check-plugin-private-value'
    monkeypatch.setenv('PROVIDER_CHECK_PLUGIN_SECRET', protected_value)
    path = _write_plugin(
        tmp_path,
        """
async def callback(ctx):
    pass

def register(reg):
    reg.add_callback(id='only', callback=callback)
""",
    )
    group = _group(
        (_plugin(path, 'provider-check', credentials='plugin-creds'),), execution='async'
    )
    sibling_plugin = _plugin(
        path,
        'sibling-secret',
        credentials='plugin-creds',
        secrets={'token': SecretRef(env='PROVIDER_CHECK_PLUGIN_SECRET')},
    )
    sibling_group = GroupConfig(
        name='sibling',
        source='events',
        plugins=(sibling_plugin,),
        callback_credentials='default-creds',
    )
    original_site = _site(group)
    site = SiteConfig(
        name=original_site.name,
        server=original_site.server,
        default_source=original_site.default_source,
        credentials=original_site.credentials,
        sources=original_site.sources,
        groups=MappingProxyType(
            {
                group.name: group,
                sibling_group.name: sibling_group,
            }
        ),
        workers=original_site.workers,
    )
    credential = site.credentials['plugin-creds'].api_key.reveal()
    provider, fake = _provider(async_=False)
    fake.name = f'provider exposed {credential} {protected_value}'
    failure: PluginError | None = None

    with _daemon_log_capture() as (logger, stream):
        try:
            load_plugins(site, group, provider)
        except PluginError as error:
            failure = error
            logger.exception('worker provider diagnostic')
        else:
            pytest.fail('provider validation accepted an incompatible provider')
        output = stream.getvalue()

    assert failure is not None
    surfaces = (str(failure), output)
    assert failure.code == 'provider-incompatible'
    assert all(credential not in surface for surface in surfaces)
    assert all(protected_value not in surface for surface in surfaces)
    assert all('<redacted>' in surface for surface in surfaces)


def test_registrar_exposes_only_immutable_config_and_freezes_after_registration(
    tmp_path: Path,
) -> None:
    mutable_config: dict[str, object] = {
        'nested': {'enabled': True},
        'values': ['one', 'two'],
    }
    path = _write_plugin(
        tmp_path,
        """
saved_registrar = None
registration_view = None
config_assignment_rejected = False

def callback(ctx):
    pass

def register(reg):
    global config_assignment_rejected, registration_view, saved_registrar
    assert not hasattr(reg, 'credentials')
    assert not hasattr(reg, 'logger')
    assert not hasattr(reg, 'sg')
    registration_view = reg.config
    try:
        reg.config = {}
    except AttributeError:
        config_assignment_rejected = True
    saved_registrar = reg
    reg.add_callback(id='only', callback=callback)
""",
    )
    group = _group((_plugin(path, 'immutable', config=mutable_config),))
    provider, _fake = _provider()

    plugin = load_plugins(_site(group), group, provider)[0]
    module = _module(plugin)
    mutable_config['nested'] = {'enabled': False}
    mutable_config['values'] = ['changed']

    assert plugin.config == {
        'nested': {'enabled': True},
        'values': ('one', 'two'),
    }
    assert module.config_assignment_rejected is True
    assert module.registration_view is plugin.config
    with pytest.raises(TypeError):
        cast('dict[str, object]', plugin.config)['new'] = True
    with pytest.raises(TypeError):
        cast('dict[str, object]', plugin.config['nested'])['enabled'] = False
    with pytest.raises(PluginError, match='registration is frozen'):
        module.saved_registrar.add_callback(id='late', callback=plugin.callbacks[0].callback)


@pytest.mark.parametrize(
    ('source', 'match'),
    [
        ('VALUE = 1\n', 'missing callable register'),
        (
            "def register(reg):\n    raise RuntimeError('registration broke')\n",
            'registration broke',
        ),
        (
            'def register(reg):\n    reg.sg.find("Shot", [], [])\n',
            'registration failed',
        ),
    ],
)
def test_registration_failures_explain_trusted_code_boundary(
    tmp_path: Path,
    source: str,
    match: str,
) -> None:
    path = _write_plugin(tmp_path, source)
    group = _group((_plugin(path, 'broken'),))
    provider, _fake = _provider()

    with pytest.raises(PluginError, match=match) as raised:
        load_plugins(_site(group), group, provider)

    text = str(raised.value)
    assert 'trusted plugin' in text
    assert 'top-level Python' in text
    assert 'not sandboxed' in text
    assert 'side-effect-free' in text


def test_registration_preserves_declared_order_and_explicit_ids(tmp_path: Path) -> None:
    path = _write_plugin(
        tmp_path,
        """
def first(ctx):
    pass

def second(ctx):
    pass

def register(reg):
    reg.add_callback(id='z-last', callback=first)
    reg.add_callback(id='a-first', callback=second)
""",
    )
    group = _group((_plugin(path, 'ordered'),))
    provider, _fake = _provider()

    plugins = load_plugins(_site(group), group, provider)

    assert tuple(callback.id for callback in plugins[0].callbacks) == ('z-last', 'a-first')
    assert tuple(callback.identity.ordinal for callback in plugins[0].callbacks) == (0, 1)


@pytest.mark.parametrize(
    'source',
    [
        """
async def register(reg):
    reg.add_callback(id='never', callback=lambda ctx: None)
""",
        """
async def registration_result():
    return None

def register(reg):
    return registration_result()
""",
    ],
)
def test_registration_rejects_async_functions_and_awaitable_results(
    tmp_path: Path,
    source: str,
) -> None:
    path = _write_plugin(tmp_path, source)
    group = _group((_plugin(path, 'async-registration'),))
    provider, _fake = _provider()

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter('always')
        with pytest.raises(PluginError, match='registration must be synchronous'):
            load_plugins(_site(group), group, provider)

    assert not [warning for warning in captured if 'was never awaited' in str(warning.message)]


def test_registration_rejects_duplicate_callback_ids(tmp_path: Path) -> None:
    path = _write_plugin(
        tmp_path,
        """
def callback(ctx):
    pass

def register(reg):
    reg.add_callback(id='same', callback=callback)
    reg.add_callback(id='same', callback=callback)
""",
    )
    group = _group((_plugin(path, 'duplicate'),))
    provider, _fake = _provider()

    with pytest.raises(PluginError, match=r'duplicate callback id.*same'):
        load_plugins(_site(group), group, provider)


def test_secret_filter_concurrent_cold_start_constructs_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_filter_type = cast(
        'type[logging.Filter]',
        vars(plugins_module)['_SecretFilter'],
    )
    secret_filter_factory = cast(
        'Callable[[], logging.Filter]',
        vars(plugins_module)['_secret_filter'],
    )
    construction_entrants = threading.Barrier(2)
    initialization_lock = _SynchronizedLock(threading.Barrier(2))
    constructions: list[logging.Filter] = []

    def construct_filter() -> logging.Filter:
        instance = original_filter_type()
        constructions.append(instance)
        if not initialization_lock._held:
            construction_entrants.wait(timeout=2)
        return instance

    monkeypatch.setattr(plugins_module, '_SECRET_FILTER_STATE', [])
    monkeypatch.setattr(
        plugins_module,
        '_SECRET_FILTER_LOCK',
        initialization_lock,
    )
    monkeypatch.setattr(plugins_module, '_SecretFilter', construct_filter)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(secret_filter_factory) for _index in range(2)]
        filters = [future.result(timeout=3) for future in futures]

    assert filters[0] is filters[1]
    assert constructions == [filters[0]]


def test_secrets_resolve_after_registration_and_are_redacted_everywhere(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protected_value = 'resolved-plugin-value'
    secret_digest = hashlib.sha256(protected_value.encode()).hexdigest()
    monkeypatch.setenv('PLUGIN_SECRET', protected_value)
    path = _write_plugin(
        tmp_path,
        """
import hashlib

setup_context = None

def setup(ctx):
    global setup_context
    setup_context = ctx
    value = ctx.secrets['token'].reveal()
    digest = hashlib.sha256(value.encode()).hexdigest()
    ctx.logger.warning(
        'nested secret: %s',
        {'items': [value]},
        extra={'plugin_details': {'digest': digest, 'value': value}},
    )

def callback(ctx):
    value = ctx.secrets['token'].reveal()
    ctx.logger.error('callback secret: %s', value)
    digest = hashlib.sha256(value.encode()).hexdigest()
    raise RuntimeError(f'callback failed with {value} {digest}')

def register(reg):
    reg.set_setup(setup)
    reg.add_callback(id='secret-callback', callback=callback)
""",
    )
    group = _group(
        (
            _plugin(
                path,
                'secret-plugin',
                secrets={'token': SecretRef(env='PLUGIN_SECRET')},
            ),
        )
    )
    site = _site(group)
    provider, _fake = _provider()
    plugins = load_plugins(site, group, provider)
    assert len(plugins) == 1
    assert plugins[0].id == 'secret-plugin'
    assert [callback.identity.callback_id for callback in plugins[0].callbacks] == [
        'secret-callback'
    ]
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    structured_handler = _StructuredHandler()
    logging.getLogger('sg_event_daemon.plugin').addHandler(handler)
    logging.getLogger('sg_event_daemon.plugin').addHandler(structured_handler)
    host = _host(site, group, provider, plugins)

    try:
        assert host.setup_sync() is None
        failure = host.invoke_sync(
            plugins[0],
            plugins[0].callbacks[0],
            _event(None),
            delivery_attempt=1,
            is_operator_replay=False,
        )
    finally:
        logging.getLogger('sg_event_daemon.plugin').removeHandler(handler)
        logging.getLogger('sg_event_daemon.plugin').removeHandler(structured_handler)

    context = cast('SetupContext', _module(plugins[0]).setup_context)
    wrapped = context.secrets['token']
    assert isinstance(wrapped, SecretValue)
    assert repr(wrapped) == '<redacted>'
    assert str(wrapped) == '<redacted>'
    assert wrapped.reveal() == protected_value
    with pytest.raises(TypeError):
        cast('dict[str, SecretValue]', context.secrets)['other'] = SecretValue('other')
    assert plugins[0].callbacks[0].identity.secret_refs == (('token', 'PLUGIN_SECRET'),)
    assert isinstance(failure, PluginError)
    assert failure.code == 'callback-failed'

    surfaces = (
        stream.getvalue(),
        str(failure),
        CallbackManifest(callbacks=(plugins[0].callbacks[0].identity,)).to_json(),
        *structured_handler._payloads,
    )
    assert '<redacted>' in stream.getvalue()
    assert structured_handler._payloads
    assert all(protected_value not in surface for surface in surfaces)
    assert all(secret_digest not in surface for surface in surfaces)


def test_setup_failure_returns_structured_group_halt_and_stops_later_setup(
    tmp_path: Path,
) -> None:
    failing = _write_plugin(
        tmp_path,
        """
def setup(ctx):
    raise RuntimeError('setup failed')

def register(reg):
    reg.set_setup(setup)
""",
        name='failing.py',
    )
    later = _write_plugin(
        tmp_path,
        """
setup_ran = False

def setup(ctx):
    global setup_ran
    setup_ran = True

def register(reg):
    reg.set_setup(setup)
""",
        name='later.py',
    )
    group = _group((_plugin(failing, 'failing'), _plugin(later, 'later')))
    site = _site(group)
    provider, fake = _provider()
    plugins = load_plugins(site, group, provider)

    halt = _host(site, group, provider, plugins).setup_sync()

    assert isinstance(halt, PluginError)
    assert halt.code == 'setup-failed'
    assert halt.site == 'studio'
    assert halt.group == 'editorial'
    assert halt.plugin_id == 'failing'
    assert halt.callback_id is None
    assert _module(plugins[1]).setup_ran is False
    assert fake._create_calls == [('sync', _SITE_URL, 'default-creds')]


def test_sync_callback_context_applies_and_clears_session_uuid(tmp_path: Path) -> None:
    path = _write_plugin(
        tmp_path,
        """
contexts = []

def callback(ctx):
    contexts.append(ctx)
    ctx.heartbeat()

def register(reg):
    reg.add_callback(
        id='change-shot',
        callback=callback,
        events={'Shotgun_Shot_Change': ('sg_status_list',)},
    )
""",
    )
    group = _group((_plugin(path, 'session'),))
    site = _site(group)
    provider, fake = _provider()
    plugins = load_plugins(site, group, provider)
    beats: list[str] = []
    host = _host(
        site,
        group,
        provider,
        plugins,
        (lambda: beats.append('beat'), lambda: False),
    )
    callback = plugins[0].callbacks[0]
    assert host.setup_sync() is None

    first = host.invoke_sync(
        plugins[0],
        callback,
        _event('browser-session'),
        delivery_attempt=3,
        is_operator_replay=True,
    )
    second = host.invoke_sync(
        plugins[0],
        callback,
        _event(None),
        delivery_attempt=1,
        is_operator_replay=False,
    )

    assert first is None
    assert second is None
    handle = cast('_FakeSyncHandle', fake._handles[0])
    assert handle._session_uuids == ['browser-session', None]
    contexts = cast('list[EventContext]', _module(plugins[0]).contexts)
    assert tuple(context.event['session_uuid'] for context in contexts) == (
        'browser-session',
        None,
    )
    assert (
        contexts[0].site,
        contexts[0].source,
        contexts[0].group,
        contexts[0].plugin_id,
        contexts[0].callback_id,
        contexts[0].delivery_attempt,
        contexts[0].is_operator_replay,
        contexts[0].shutdown_requested,
    ) == ('studio', 'events', 'editorial', 'session', 'change-shot', 3, True, False)
    assert contexts[0].config is plugins[0].config
    assert contexts[0].sg is handle
    assert beats == ['beat', 'beat']
    assert host.matching_callbacks(_event(None)) == ((plugins[0], callback),)
    assert host.matching_callbacks(_event(None, event_type='Shotgun_Task_Change')) == ()


def test_sync_host_creates_independent_handles_and_fixed_setup_contexts(
    tmp_path: Path,
) -> None:
    path = _write_plugin(
        tmp_path,
        """
setup_contexts = []

def setup(ctx):
    setup_contexts.append(ctx)
    ctx.heartbeat()

def callback(ctx):
    pass

def register(reg):
    reg.set_setup(setup)
    reg.add_callback(id='only', callback=callback)
""",
    )
    group = _group(
        (
            _plugin(path, 'default-plugin'),
            _plugin(path, 'override-plugin', credentials='plugin-creds'),
        )
    )
    site = _site(group)
    provider, fake = _provider()
    plugins = load_plugins(site, group, provider)
    beats: list[str] = []
    stopping = False
    host = _host(
        site,
        group,
        provider,
        plugins,
        (lambda: beats.append('beat'), lambda: stopping),
    )

    setup_results: list[PluginError | None] = []

    def run_setup() -> None:
        setup_results.append(host.setup_sync())

    lane_thread = threading.Thread(target=run_setup)
    lane_thread.start()
    lane_thread.join()

    assert setup_results == [None]
    contexts = tuple(cast('SetupContext', _module(plugin).setup_contexts[0]) for plugin in plugins)
    assert fake._create_calls == [
        ('sync', _SITE_URL, 'default-creds'),
        ('sync', _SITE_URL, 'plugin-creds'),
    ]
    assert lane_thread.ident is not None
    assert fake._create_threads == [lane_thread.ident, lane_thread.ident]
    assert len({id(context.sg) for context in contexts}) == 2
    assert beats == ['beat', 'beat']
    assert all(
        (
            context.site,
            context.source,
            context.group,
            context.plugin_id,
            context.shutdown_requested,
        )
        == ('studio', 'events', 'editorial', plugin.id, False)
        for context, plugin in zip(contexts, plugins, strict=True)
    )
    stopping = True
    assert all(context.shutdown_requested for context in contexts)
    host.close_sync()
    assert all(handle._closed for handle in fake._handles)


@pytest.mark.parametrize('result', [None, False, 0, 'value', [1], {'key': 'value'}])
def test_sync_result_validation_accepts_ordinary_values(result: object) -> None:
    validate_sync_callback_result(result, 'ordinary')


def test_sync_result_validation_rejects_custom_awaitable_without_executing_it() -> None:
    actions: list[str] = []

    class AwaitableResult:
        def __await__(self) -> Generator[object, None, None]:
            actions.append('awaited')
            yield

        def close(self) -> None:
            actions.append('closed')

    with pytest.raises(TypeError, match="sync callback 'custom' returned an awaitable"):
        validate_sync_callback_result(AwaitableResult(), 'custom')

    assert actions == []


def test_sync_session_setter_failure_returns_redacted_group_halt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protected_value = 'sync-setter-private-value'
    protected_digest = hashlib.sha256(protected_value.encode()).hexdigest()
    monkeypatch.setenv('SETTER_PRIVATE_VALUE', protected_value)
    path = _write_plugin(
        tmp_path,
        """
called = False

def callback(ctx):
    global called
    called = True

def register(reg):
    reg.add_callback(id='only', callback=callback)
""",
    )
    group = _group(
        (
            _plugin(
                path,
                'setter-failure',
                secrets={'private': SecretRef(env='SETTER_PRIVATE_VALUE')},
            ),
        )
    )
    site = _site(group)
    provider, fake = _provider()
    plugins = load_plugins(site, group, provider)
    host = _host(site, group, provider, plugins)
    assert host.setup_sync() is None
    handle = cast('_FakeSyncHandle', fake._handles[0])
    handle._session_error = RuntimeError(f'setter exposed {protected_value} {protected_digest}')

    failure = host.invoke_sync(
        plugins[0],
        plugins[0].callbacks[0],
        _event('browser-session'),
        delivery_attempt=1,
        is_operator_replay=False,
    )

    assert isinstance(failure, PluginError)
    assert failure.code == 'callback-failed'
    assert failure.callback_id == 'only'
    assert protected_value not in str(failure)
    assert protected_digest not in str(failure)
    assert _module(plugins[0]).called is False


def test_with_heartbeat_preserves_event_context_and_updates_callback() -> None:
    called: list[str] = []
    context = EventContext(
        sg=cast('SyncSG', object()),
        logger=logging.getLogger('test-plugin-context'),
        config=MappingProxyType({}),
        secrets=MappingProxyType({}),
        site='studio',
        source='events',
        group='editorial',
        plugin_id='notify',
        _heartbeat_callback=lambda: called.append('old'),
        _shutdown_check=lambda: False,
        event=cast('Event', object()),
        callback_id='update',
        delivery_attempt=2,
        is_operator_replay=True,
    )

    updated = with_heartbeat(context, lambda: called.append('new'))
    updated.heartbeat()
    context.heartbeat()

    assert isinstance(updated, EventContext)
    assert updated is not context
    assert updated.event is context.event
    assert updated.delivery_attempt == 2
    assert updated.is_operator_replay is True
    assert called == ['new', 'old']
