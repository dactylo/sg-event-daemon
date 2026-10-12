"""Installed-distribution validation and lazy SG handle factories."""

from __future__ import annotations

import asyncio
import inspect
import re
from dataclasses import dataclass, field
from importlib import import_module, metadata
from importlib.util import find_spec
from typing import TYPE_CHECKING, Protocol, TypedDict, Unpack, cast, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable

    from ._config import CredentialConfig

_PROVIDER_NAME_SEPARATOR = re.compile(r'[-_.]+')
REQUIRED_HANDLE_METHODS = ('close', 'find', 'set_session_uuid')
SUPPORTED_PROVIDERS = frozenset({'sg-python-api', 'shotgun-api3'})


@dataclass(frozen=True)
class ProviderCapabilities:
    """Record capabilities validated at the provider import boundary."""

    sync: bool
    async_: bool


class ProviderError(RuntimeError):
    """Report invalid provider installation identity or capabilities."""


class _AsyncFactory(Protocol):
    """Construct one provider-specific async handle."""

    def __call__(
        self,
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> object:
        """Construct one handle without connecting."""
        ...


class _FindOptions(TypedDict, total=False):
    """Describe optional query settings shared by both providers."""

    limit: int
    order: list[dict[str, str]] | None
    page: int


@runtime_checkable
class AsyncSG(Protocol):
    """Define the asynchronous SG surface owned by the daemon."""

    async def close(self) -> None:
        """Release resources owned by this handle."""
        ...

    async def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **kwargs: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        """Find provider records required by daemon polling.

        Args:
            entity_type: Provider entity type to query.
            filters: Provider-compatible filter expression.
            fields: Fields to return.
            **kwargs: Daemon-owned optional query settings supported by both
                providers.

        Returns:
            Provider records in server order.
        """
        ...

    def set_session_uuid(self, session_uuid: str | None) -> None:
        """Apply or clear the session UUID used by later requests.

        Args:
            session_uuid: Browser session UUID, or `None` to clear it.
        """
        ...


@runtime_checkable
class SyncSG(Protocol):
    """Define the synchronous SG surface owned by the daemon."""

    def close(self) -> None:
        """Release resources owned by this handle."""
        ...

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **kwargs: Unpack[_FindOptions],
    ) -> list[dict[str, object]]:
        """Find provider records required by daemon polling.

        Args:
            entity_type: Provider entity type to query.
            filters: Provider-compatible filter expression.
            fields: Fields to return.
            **kwargs: Daemon-owned optional query settings supported by both
                providers.

        Returns:
            Provider records in server order.
        """
        ...

    def set_session_uuid(self, session_uuid: str | None) -> None:
        """Apply or clear the session UUID used by later requests.

        Args:
            session_uuid: Browser session UUID, or `None` to clear it.
        """
        ...


class ProviderInterface(Protocol):
    """Expose only the provider operations consumed by plugin hosts."""

    async def create_async(self, site: str, credential: CredentialConfig) -> AsyncSG:
        """Create one asynchronous handle for the supplied credential."""
        ...

    def create_sync(self, site: str, credential: CredentialConfig) -> SyncSG:
        """Create one synchronous handle for the supplied credential."""
        ...

    @property
    def name(self) -> str:
        """Return the selected provider name."""
        ...

    @property
    def capabilities(self) -> ProviderCapabilities:
        """Return the provider's supported handle modes."""
        ...


class _ProviderConfig(Protocol):
    """Expose common mutable settings shared by both provider families."""

    max_rpc_attempts: int
    rpc_attempt_interval: int
    timeout_secs: float


class _ConfigurableHandle(Protocol):
    """Expose the provider configuration object on one constructed handle."""

    config: _ProviderConfig


class _SyncFactory(Protocol):
    """Construct one provider-specific synchronous handle."""

    def __call__(
        self,
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> object:
        """Construct one handle without connecting."""
        ...


@dataclass(frozen=True)
class Provider:
    """Create independently configured handles for one validated provider."""

    name: str
    capabilities: ProviderCapabilities
    _sync_factory: _SyncFactory = field(repr=False)
    _async_factory: _AsyncFactory | None = field(repr=False)

    async def create_async(self, site: str, credential: CredentialConfig) -> AsyncSG:
        """Create one independently configured lazy async handle.

        Args:
            site: SG site URL.
            credential: Resolved script credential and request settings.

        Returns:
            A handle constructed on the current running event loop.

        Raises:
            ProviderError: The selected provider has no async capability or
                returns an incompatible handle.
        """
        asyncio.get_running_loop()
        if self._async_factory is None:
            raise ProviderError(f'provider {self.name} does not support async handles')
        handle = self._async_factory(
            site,
            script_name=credential.script_name,
            api_key=credential.api_key.reveal(),
            connect=False,
        )
        _validate_handle_mode(handle, self.name, async_=True)
        _configure_handle(handle, credential)
        return cast('AsyncSG', handle)

    def create_sync(self, site: str, credential: CredentialConfig) -> SyncSG:
        """Create one independently configured lazy synchronous handle.

        Args:
            site: SG site URL.
            credential: Resolved script credential and request settings.

        Returns:
            An independently constructed synchronous handle.

        Raises:
            ProviderError: The provider returns an incompatible handle.
        """
        handle = self._sync_factory(
            site,
            script_name=credential.script_name,
            api_key=credential.api_key.reveal(),
            connect=False,
        )
        _validate_handle_mode(handle, self.name, async_=False)
        _configure_handle(handle, credential)
        return cast('SyncSG', handle)


def _configure_handle(handle: object, credential: CredentialConfig) -> None:
    """Apply daemon-owned request settings before the first provider call."""
    try:
        config = cast('_ConfigurableHandle', handle).config
        config.timeout_secs = credential.timeout_secs
        config.max_rpc_attempts = credential.max_rpc_attempts
        config.rpc_attempt_interval = round(credential.rpc_attempt_interval_secs * 1_000)
    except (AttributeError, TypeError, ValueError) as error:
        raise ProviderError('provider handle does not expose common request settings') from error


def _factory(
    module: object,
    export: str,
    methods: tuple[str, ...],
) -> object | None:
    """Return one callable provider export with the required class surface."""
    value = getattr(module, export, None)
    if not callable(value):
        return None
    if not all(callable(getattr(value, method, None)) for method in methods):
        return None
    return value


def _normalize_provider_name(name: str) -> str:
    """Return the canonical distribution spelling used by provider policy."""
    return _PROVIDER_NAME_SEPARATOR.sub('-', name).lower()


def _namespace_owner_names() -> tuple[str, ...]:
    """Return every normalized distribution claiming `shotgun_api3`."""
    owners = metadata.packages_distributions().get('shotgun_api3', ())
    return tuple(sorted({_normalize_provider_name(name) for name in owners}))


def _installed_provider_names(owners: Collection[str] | None = None) -> tuple[str, ...]:
    """Return supported distributions attributed to `shotgun_api3`."""
    if owners is None:
        owners = _namespace_owner_names()
    return tuple(name for name in owners if name in SUPPORTED_PROVIDERS)


def callable_mode(value: object) -> bool | None:
    """Classify a callable's declared execution mode without invoking it.

    Returns:
        `True` for async, `False` for sync, or `None` for an invalid callable.
    """
    if not callable(value):
        return None
    try:
        targets = (inspect.unwrap(value), inspect.unwrap(value.__call__))
    except (AttributeError, ValueError):
        return None
    return any(inspect.iscoroutinefunction(target) for target in targets)


def _validate_handle_mode(handle: object, provider_name: str, *, async_: bool) -> None:
    """Reject a structurally or execution-mode incompatible provider handle."""
    structurally_compatible = isinstance(handle, AsyncSG) if async_ else isinstance(handle, SyncSG)
    mode_compatible = structurally_compatible and all(
        callable_mode(getattr(handle, method)) is expected_async
        for method, expected_async in (
            ('find', async_),
            ('close', async_),
            ('set_session_uuid', False),
        )
    )
    if not mode_compatible:
        mode = 'async' if async_ else 'sync'
        raise ProviderError(f'provider {provider_name} returned an incompatible {mode} handle')


def load_provider(expected: str, required: Iterable[str]) -> Provider:
    """Validate provider identity and lazily import its shared namespace.

    Args:
        expected: Configured provider distribution name.
        required: Required capabilities, using `sync` and `async`.

    Returns:
        A provider whose exports match the required daemon surface.

    Raises:
        ProviderError: Installation identity, attribution, or exports are
            incompatible with the configured provider and required modes.
    """
    expected_name = _normalize_provider_name(expected)
    if expected_name not in SUPPORTED_PROVIDERS:
        raise ProviderError(f'unsupported provider {expected!r}')
    required_names = frozenset(required)
    unknown = sorted(required_names - {'async', 'sync'})
    if unknown:
        raise ProviderError(f'unknown provider capabilities: {", ".join(unknown)}')

    owners = _namespace_owner_names()
    unknown_owners = tuple(name for name in owners if name not in SUPPORTED_PROVIDERS)
    if unknown_owners:
        raise ProviderError(
            'shotgun_api3 namespace is not attributed exclusively to a supported '
            f'distribution: {", ".join(unknown_owners)}'
        )
    installed = _installed_provider_names(owners)
    if len(installed) > 1:
        raise ProviderError('both shotgun-api3 and sg-python-api own the shotgun_api3 namespace')
    if not installed:
        if find_spec('shotgun_api3') is not None:
            raise ProviderError(
                'shotgun_api3 namespace is present but not attributed to a supported distribution'
            )
        raise ProviderError(f'selected provider {expected_name} is not installed')
    if installed[0] != expected_name:
        raise ProviderError(f'selected provider {expected_name} is not installed')

    module = import_module('shotgun_api3')
    sync_export = _factory(module, 'Shotgun', REQUIRED_HANDLE_METHODS)
    if sync_export is None:
        raise ProviderError(f'provider {expected_name} does not export a compatible Shotgun')
    async_export = _factory(module, 'AsyncShotgun', REQUIRED_HANDLE_METHODS)
    capabilities = ProviderCapabilities(
        sync=True,
        async_=async_export is not None,
    )
    if 'async' in required_names and not capabilities.async_:
        raise ProviderError(f'provider {expected_name} does not export AsyncShotgun')
    return Provider(
        name=expected_name,
        capabilities=capabilities,
        _sync_factory=cast('_SyncFactory', sync_export),
        _async_factory=cast('_AsyncFactory | None', async_export),
    )
