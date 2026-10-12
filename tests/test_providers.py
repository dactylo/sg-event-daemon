from __future__ import annotations

import asyncio
import functools
from types import MethodType, SimpleNamespace
from typing import ClassVar, cast

import pytest

import sg_event_daemon._providers as providers
from sg_event_daemon._config import CredentialConfig, SecretValue
from sg_event_daemon._providers import (
    AsyncSG,
    Provider,
    ProviderError,
    SyncSG,
    _installed_provider_names,
    load_provider,
)

_SITE = 'https://example.invalid'


class _AsyncCallableMethod:
    def __init__(self) -> None:
        self._called = False

    async def __call__(self, *_args: object, **_kwargs: object) -> object:
        self._called = True
        raise AssertionError('provider method ran during handle validation')


class _FakeAsyncShotgun:
    _calls: ClassVar[list[tuple[str, bool]]] = []
    _handles: ClassVar[list[_FakeAsyncShotgun]] = []

    def __init__(
        self,
        _site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> None:
        assert script_name == 'daemon'
        assert api_key == 'secret'
        self.config = _FakeConfig()
        self._constructed_loop = asyncio.get_running_loop()
        self._closed = False
        self._session_uuid: str | None = None
        type(self)._calls.append(('AsyncShotgun', connect))
        type(self)._handles.append(self)

    async def close(self) -> None:
        self._closed = True

    async def find(
        self,
        _entity_type: str,
        _filters: object,
        _fields: list[str] | None = None,
        _order: list[dict[str, str]] | None = None,
        *,
        limit: int = 0,
        page: int = 0,
    ) -> list[dict[str, object]]:
        assert limit >= 0
        assert page >= 0
        return []

    def set_session_uuid(self, session_uuid: str | None) -> None:
        self._session_uuid = session_uuid


class _FakeConfig:
    def __init__(self) -> None:
        self.max_rpc_attempts = 0
        self.rpc_attempt_interval = 0
        self.timeout_secs = 0.0


class _FakeShotgun:
    _calls: ClassVar[list[tuple[str, bool]]] = []
    _handles: ClassVar[list[_FakeShotgun]] = []

    def __init__(
        self,
        _site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> None:
        assert script_name == 'daemon'
        assert api_key == 'secret'
        self.config = _FakeConfig()
        self._closed = False
        self._session_uuid: str | None = None
        type(self)._calls.append(('Shotgun', connect))
        type(self)._handles.append(self)

    def close(self) -> None:
        self._closed = True

    def find(
        self,
        _entity_type: str,
        _filters: object,
        _fields: list[str] | None = None,
        _order: list[dict[str, str]] | None = None,
        *,
        limit: int = 0,
        page: int = 0,
    ) -> list[dict[str, object]]:
        assert limit >= 0
        assert page >= 0
        return []

    def set_session_uuid(self, session_uuid: str | None) -> None:
        self._session_uuid = session_uuid


class _ReturningFactory:
    def __init__(self, handle: object) -> None:
        self._handle = handle

    def __call__(
        self,
        _site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> object:
        assert script_name == 'daemon'
        assert api_key == 'secret'
        assert connect is False
        return self._handle

    def close(self) -> None:
        raise AssertionError('factory surface method must not run')

    def find(
        self,
        _entity_type: str,
        _filters: object,
        _fields: list[str] | None = None,
        _order: list[dict[str, str]] | None = None,
        *,
        _limit: int = 0,
        _page: int = 0,
    ) -> list[dict[str, object]]:
        raise AssertionError('factory surface method must not run')

    def set_session_uuid(self, _session_uuid: str | None) -> None:
        raise AssertionError('factory surface method must not run')


def _credential() -> CredentialConfig:
    return CredentialConfig(
        name='admin',
        script_name='daemon',
        api_key=SecretValue('secret'),
        timeout_secs=17.0,
        max_rpc_attempts=4,
        rpc_attempt_interval_secs=2.5,
    )


def _install_fake_environment(
    monkeypatch: pytest.MonkeyPatch,
    *,
    module: object | None = None,
    owners: list[str] | None = None,
    namespace_present: bool = True,
) -> list[str]:
    imports: list[str] = []
    installed = ['shotgun-api3'] if owners is None else owners
    fake_module = (
        SimpleNamespace(Shotgun=_FakeShotgun, AsyncShotgun=_FakeAsyncShotgun)
        if module is None
        else module
    )
    monkeypatch.setattr(
        providers.metadata,
        'packages_distributions',
        lambda: {'shotgun_api3': installed} if installed else {},
    )

    def find_fake_spec(_name: str) -> object | None:
        return object() if namespace_present else None

    monkeypatch.setattr(
        providers,
        'find_spec',
        find_fake_spec,
    )

    def import_fake(name: str) -> object:
        imports.append(name)
        assert name == 'shotgun_api3'
        return fake_module

    monkeypatch.setattr(providers, 'import_module', import_fake)
    return imports


def _load_returning_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Provider, _FakeShotgun, _FakeAsyncShotgun]:
    sync_handle = _FakeShotgun(
        _SITE,
        script_name='daemon',
        api_key='secret',
        connect=False,
    )
    async_handle = _FakeAsyncShotgun(
        _SITE,
        script_name='daemon',
        api_key='secret',
        connect=False,
    )
    _install_fake_environment(
        monkeypatch,
        module=SimpleNamespace(
            Shotgun=_ReturningFactory(sync_handle),
            AsyncShotgun=_ReturningFactory(async_handle),
        ),
        owners=['sg-python-api'],
    )
    return load_provider('sg-python-api', {'async', 'sync'}), sync_handle, async_handle


def test_async_factory_constructs_on_running_loop_and_applies_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FakeAsyncShotgun._calls.clear()
    _FakeAsyncShotgun._handles.clear()
    _install_fake_environment(monkeypatch, owners=['sg-python-api'])
    provider = load_provider('sg-python-api', {'async', 'sync'})

    async def create() -> tuple[tuple[AsyncSG, ...], asyncio.AbstractEventLoop]:
        loop = asyncio.get_running_loop()
        handles = tuple([await provider.create_async(_SITE, _credential()) for _index in range(3)])
        return handles, loop

    handles, loop = asyncio.run(create())

    assert all(isinstance(handle, AsyncSG) for handle in handles)
    assert len({id(handle) for handle in handles}) == 3
    assert _FakeAsyncShotgun._calls == [
        ('AsyncShotgun', False),
        ('AsyncShotgun', False),
        ('AsyncShotgun', False),
    ]
    for handle in handles:
        concrete = cast('_FakeAsyncShotgun', handle)
        assert concrete._constructed_loop is loop
        assert concrete.config.timeout_secs == 17.0
        assert concrete.config.max_rpc_attempts == 4
        assert concrete.config.rpc_attempt_interval == 2_500


def test_async_factory_rejects_sync_methods_before_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FakeShotgun._handles.clear()
    _install_fake_environment(
        monkeypatch,
        module=SimpleNamespace(Shotgun=_FakeShotgun, AsyncShotgun=_FakeShotgun),
        owners=['sg-python-api'],
    )
    provider = load_provider('sg-python-api', {'async', 'sync'})

    async def create() -> None:
        with pytest.raises(
            ProviderError,
            match=r'provider sg-python-api.*incompatible async',
        ):
            await provider.create_async(_SITE, _credential())

    asyncio.run(create())

    config = _FakeShotgun._handles[-1].config
    assert config.timeout_secs == 0.0
    assert config.max_rpc_attempts == 0
    assert config.rpc_attempt_interval == 0


def test_async_factory_requires_declared_capability(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_environment(
        monkeypatch,
        module=SimpleNamespace(Shotgun=_FakeShotgun),
    )
    provider = load_provider('shotgun-api3', {'sync'})

    async def create() -> None:
        with pytest.raises(ProviderError, match='async'):
            await provider.create_async(_SITE, _credential())

    asyncio.run(create())


def test_capabilities_follow_validated_exports(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_environment(
        monkeypatch,
        module=SimpleNamespace(Shotgun=_FakeShotgun),
    )

    provider = load_provider('shotgun-api3', {'sync'})

    assert provider.capabilities.sync is True
    assert provider.capabilities.async_ is False


@pytest.mark.filterwarnings('error')
@pytest.mark.parametrize('method_name', ['close', 'find'])
def test_factories_classify_async_callable_object_methods(
    method_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def validate() -> None:
        provider, sync_handle, async_handle = _load_returning_provider(monkeypatch)
        sync_method = _AsyncCallableMethod()
        async_method = _AsyncCallableMethod()
        setattr(sync_handle, method_name, sync_method)
        setattr(async_handle, method_name, async_method)

        with pytest.raises(
            ProviderError,
            match=r'provider sg-python-api.*incompatible sync',
        ):
            provider.create_sync(_SITE, _credential())

        assert sync_handle.config.timeout_secs == 0.0
        assert sync_method._called is False
        assert await provider.create_async(_SITE, _credential()) is async_handle
        assert async_handle.config.timeout_secs == 17.0
        assert async_method._called is False

    asyncio.run(validate())


@pytest.mark.filterwarnings('error')
@pytest.mark.parametrize('method_name', ['close', 'find'])
def test_factories_classify_decorated_async_methods(
    method_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invocations: list[str] = []

    async def native(*_args: object, **_kwargs: object) -> object:
        invocations.append('native')
        raise AssertionError('provider method ran during handle validation')

    def decorated(*args: object, **kwargs: object) -> object:
        invocations.append('decorated')
        return native(*args, **kwargs)

    functools.update_wrapper(decorated, native)

    async def validate() -> None:
        provider, sync_handle, async_handle = _load_returning_provider(monkeypatch)
        setattr(sync_handle, method_name, MethodType(decorated, sync_handle))
        setattr(async_handle, method_name, MethodType(decorated, async_handle))

        with pytest.raises(
            ProviderError,
            match=r'provider sg-python-api.*incompatible sync',
        ):
            provider.create_sync(_SITE, _credential())

        assert sync_handle.config.timeout_secs == 0.0
        assert await provider.create_async(_SITE, _credential()) is async_handle
        assert async_handle.config.timeout_secs == 17.0
        assert invocations == []

    asyncio.run(validate())


@pytest.mark.filterwarnings('error')
@pytest.mark.parametrize('method_name', ['close', 'find'])
def test_factories_reject_non_callable_methods(
    method_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def validate() -> None:
        provider, sync_handle, async_handle = _load_returning_provider(monkeypatch)
        setattr(sync_handle, method_name, object())
        setattr(async_handle, method_name, object())

        with pytest.raises(
            ProviderError,
            match=r'provider sg-python-api.*incompatible sync',
        ):
            provider.create_sync(_SITE, _credential())
        with pytest.raises(
            ProviderError,
            match=r'provider sg-python-api.*incompatible async',
        ):
            await provider.create_async(_SITE, _credential())

        assert sync_handle.config.timeout_secs == 0.0
        assert async_handle.config.timeout_secs == 0.0

    asyncio.run(validate())


@pytest.mark.filterwarnings('error')
def test_async_handle_mode_rejects_async_session_setter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def validate() -> None:
        provider, _sync_handle, async_handle = _load_returning_provider(monkeypatch)
        setter = _AsyncCallableMethod()
        monkeypatch.setattr(async_handle, 'set_session_uuid', setter)

        with pytest.raises(
            ProviderError,
            match=r'provider sg-python-api.*incompatible async',
        ):
            await provider.create_async(_SITE, _credential())

        assert async_handle.config.timeout_secs == 0.0
        assert setter._called is False

    asyncio.run(validate())


def test_handle_modes_accept_sync_session_setter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def validate() -> None:
        provider, sync_handle, async_handle = _load_returning_provider(monkeypatch)

        assert provider.create_sync(_SITE, _credential()) is sync_handle
        assert await provider.create_async(_SITE, _credential()) is async_handle

    asyncio.run(validate())


def test_installed_provider_names_normalizes_distribution_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_environment(monkeypatch, owners=['Shotgun_API3'])

    assert _installed_provider_names() == ('shotgun-api3',)
    assert load_provider('shotgun.api3', (mode for mode in ('sync',))).name == 'shotgun-api3'


def test_load_provider_imports_only_after_metadata_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imports = _install_fake_environment(monkeypatch)

    assert imports == []

    provider = load_provider('shotgun-api3', {'sync'})

    assert provider.name == 'shotgun-api3'
    assert imports == ['shotgun_api3']


def test_load_provider_rejects_both_distributions(monkeypatch: pytest.MonkeyPatch) -> None:
    imports = _install_fake_environment(
        monkeypatch,
        owners=['shotgun-api3', 'sg_python_api'],
    )

    with pytest.raises(ProviderError, match=r'both.*shotgun-api3.*sg-python-api'):
        load_provider('shotgun-api3', {'sync'})

    assert imports == []


def test_load_provider_rejects_missing_async_export(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_environment(
        monkeypatch,
        module=SimpleNamespace(Shotgun=_FakeShotgun),
        owners=['sg-python-api'],
    )

    with pytest.raises(ProviderError, match='AsyncShotgun'):
        load_provider('sg-python-api', {'async', 'sync'})


def test_load_provider_rejects_missing_selected_distribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imports = _install_fake_environment(monkeypatch, owners=['sg-python-api'])

    with pytest.raises(ProviderError, match=r'shotgun-api3.*not installed'):
        load_provider('shotgun-api3', {'sync'})

    assert imports == []


def test_load_provider_rejects_missing_sync_export(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_environment(monkeypatch, module=SimpleNamespace())

    with pytest.raises(ProviderError, match='Shotgun'):
        load_provider('shotgun-api3', {'sync'})


def test_load_provider_rejects_unattributed_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imports = _install_fake_environment(monkeypatch, owners=[])

    with pytest.raises(ProviderError, match=r'namespace.*not attributed'):
        load_provider('shotgun-api3', {'sync'})

    assert imports == []


def test_load_provider_rejects_unknown_namespace_coowner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imports = _install_fake_environment(
        monkeypatch,
        owners=['shotgun-api3', 'custom-provider'],
    )

    with pytest.raises(ProviderError, match=r'namespace.*not attributed'):
        load_provider('shotgun-api3', {'sync'})

    assert imports == []


def test_load_provider_rejects_wholly_missing_namespace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imports = _install_fake_environment(
        monkeypatch,
        owners=[],
        namespace_present=False,
    )

    with pytest.raises(ProviderError, match=r'shotgun-api3.*not installed'):
        load_provider('shotgun-api3', {'sync'})

    assert imports == []


def test_load_provider_uses_one_owner_metadata_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_environment(monkeypatch)
    snapshots = iter(
        (
            {'shotgun_api3': ['shotgun-api3']},
            {'shotgun_api3': ['sg-python-api']},
        )
    )
    calls = 0

    def packages_distributions() -> dict[str, list[str]]:
        nonlocal calls
        calls += 1
        return next(snapshots)

    monkeypatch.setattr(providers.metadata, 'packages_distributions', packages_distributions)

    provider = load_provider('shotgun-api3', {'sync'})

    assert provider.name == 'shotgun-api3'
    assert calls == 1


def test_sync_factory_creates_independent_handles_with_daemon_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FakeShotgun._calls.clear()
    _FakeShotgun._handles.clear()
    _install_fake_environment(monkeypatch)
    provider = load_provider('shotgun-api3', {'sync'})

    handles = tuple(provider.create_sync(_SITE, _credential()) for _index in range(3))

    assert all(isinstance(handle, SyncSG) for handle in handles)
    assert len({id(handle) for handle in handles}) == 3
    assert _FakeShotgun._calls == [
        ('Shotgun', False),
        ('Shotgun', False),
        ('Shotgun', False),
    ]
    for handle in _FakeShotgun._handles:
        assert handle.config.timeout_secs == 17.0
        assert handle.config.max_rpc_attempts == 4
        assert handle.config.rpc_attempt_interval == 2_500


@pytest.mark.filterwarnings('error')
def test_sync_handle_mode_rejects_async_session_setter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def validate() -> None:
        provider, sync_handle, _async_handle = _load_returning_provider(monkeypatch)
        setter = _AsyncCallableMethod()
        monkeypatch.setattr(sync_handle, 'set_session_uuid', setter)

        with pytest.raises(
            ProviderError,
            match=r'provider sg-python-api.*incompatible sync',
        ):
            provider.create_sync(_SITE, _credential())

        assert sync_handle.config.timeout_secs == 0.0
        assert setter._called is False

    asyncio.run(validate())


@pytest.mark.parametrize(
    'method_name',
    ['close', 'find'],
)
def test_sync_factory_rejects_async_method_before_configuration(
    method_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class WrongModeShotgun(_FakeShotgun):
        pass

    monkeypatch.setattr(
        WrongModeShotgun,
        method_name,
        getattr(_FakeAsyncShotgun, method_name),
    )
    WrongModeShotgun._handles.clear()
    _install_fake_environment(
        monkeypatch,
        module=SimpleNamespace(Shotgun=WrongModeShotgun),
    )
    provider = load_provider('shotgun-api3', {'sync'})

    with pytest.raises(
        ProviderError,
        match=r'provider shotgun-api3.*incompatible sync',
    ):
        provider.create_sync(_SITE, _credential())

    config = WrongModeShotgun._handles[-1].config
    assert config.timeout_secs == 0.0
    assert config.max_rpc_attempts == 0
    assert config.rpc_attempt_interval == 0
