"""Load and drive one trusted plugin without a daemon or live SG connection.

Use `load_plugin` with `HarnessSettings` to register a plugin file, then pass
synthetic EventLogEntry mappings to `drive_sync_plugin` or
`drive_async_plugin`.
The default SG handle rejects requests; callers may inject a
protocol-compatible handle. The harness owns and closes an acquired handle
after each drive.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType, ModuleType
from typing import TYPE_CHECKING, Literal, Protocol, cast

from ._config import (
    CredentialConfig,
    GroupConfig,
    PluginConfig,
    SecretRef,
    SecretValue,
    SiteConfig,
)
from ._events import Event
from ._plugins import (
    CallbackSpec,
    PluginError,
    PluginHost,
    PluginSpec,
    load_plugin_spec,
)
from ._providers import (
    AsyncSG,
    ProviderCapabilities,
    ProviderInterface,
    SyncSG,
    callable_mode,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = [
    'CallbackResult',
    'DriveResult',
    'FailureReport',
    'HarnessHandle',
    'HarnessSettings',
    'LoadedPlugin',
    'PluginError',
    'drive_async_plugin',
    'drive_sync_plugin',
    'load_plugin',
]

_EMPTY_CONFIG = cast('Mapping[str, object]', MappingProxyType({}))
_EMPTY_SECRETS = cast('Mapping[str, str]', MappingProxyType({}))
_HARNESS_CREDENTIAL = 'testing'
_HARNESS_KEY = 'offline-testing-key'
_HARNESS_SERVER = 'https://testing.invalid'
_OFFLINE_GUIDANCE = 'inject sg= to make SG requests from the plugin harness'


@dataclass(frozen=True)
class FailureReport:
    """Describe one redacted setup or callback failure.

    Attributes:
        code: Machine-readable reason: `secret-missing`, `event-invalid`,
            `setup-failed`, or `callback-failed`.
        detail: Redacted `PluginError` text containing the code, available
            identities, and the trusted-plugin notice.
        site: Site name associated with the failure.
        group: Group name associated with the failure.
        plugin_id: Plugin name, or `None` for a group-level failure.
        callback_id: Callback name, or `None` for a setup failure.
    """

    code: str
    detail: str
    site: str
    group: str
    plugin_id: str | None
    callback_id: str | None


@dataclass(frozen=True)
class CallbackResult:
    """Describe one matching callback invocation.

    Attributes:
        event_id: Positive synthetic event ID supplied to the callback.
        plugin_id: Plugin whose callback matched the event.
        callback_id: Matching callback name.
        delivery_attempt: One-based attempt supplied to the callback context.
        failure: Redacted callback failure, or `None` on success.
    """

    event_id: int
    plugin_id: str
    callback_id: str
    delivery_attempt: int
    failure: FailureReport | None


@dataclass(frozen=True)
class DriveResult:
    """Contain immutable setup and matching-callback results.

    Attributes:
        setup_failure: Redacted setup failure, or `None` when setup succeeded.
        callbacks: Ordered results for matching callbacks that ran, ending with
            the first failure if any.
        ok: Whether setup and every invoked callback succeeded.
    """

    setup_failure: FailureReport | None
    callbacks: tuple[CallbackResult, ...]

    @property
    def ok(self) -> bool:
        """Return whether setup and every matching callback succeeded.

        Returns:
            `True` when the complete drive succeeded.
        """
        return self.setup_failure is None and all(
            callback.failure is None for callback in self.callbacks
        )


class HarnessHandle(Protocol):
    """Provide the lifecycle methods required of an injected SG handle.

    Plugin-specific SG operations remain duck typed and may be supplied by
    either a synchronous or asynchronous handle.
    """

    def close(self) -> object:
        """Release the handle in the plugin's execution mode."""
        ...

    def set_session_uuid(self, session_uuid: str | None, /) -> object:
        """Set or clear the browser session for subsequent requests.

        This method is synchronous in both plugin execution modes.

        Args:
            session_uuid: Session UUID or `None` to clear it.
        """
        ...


@dataclass(frozen=True)
class HarnessSettings:
    """Define public context, config, and secret references for one plugin.

    Attributes:
        site: Site name passed to plugin contexts.
        source: Source name passed to plugin contexts.
        group: Group name passed to plugin contexts.
        config: Non-secret JSON-compatible values copied and deeply frozen
            during loading.
        secrets: Context secret names mapped to environment variable names;
            their values are resolved during drive setup.
    """

    site: str = 'testing'
    source: str = 'events'
    group: str = 'testing'
    config: Mapping[str, object] = field(default_factory=lambda: _EMPTY_CONFIG)
    secrets: Mapping[str, str] = field(default_factory=lambda: _EMPTY_SECRETS)


@dataclass(frozen=True)
class LoadedPlugin:
    """Contain one loaded plugin and its public offline-harness identity.

    Attributes:
        plugin_id: Stable ID assigned while loading the plugin.
        execution: Synchronous or asynchronous callback mode.
        site: Site name supplied to contexts.
        source: Source name supplied to contexts.
        group: Group name supplied to contexts.
        callbacks: Registered callback IDs in declaration order.
        config: Deeply frozen non-secret plugin configuration.
        module: Imported trusted plugin module for test inspection.
    """

    plugin_id: str
    execution: Literal['sync', 'async']
    site: str
    source: str
    group: str
    callbacks: tuple[str, ...]
    config: Mapping[str, object]
    module: ModuleType = field(repr=False)
    _site_config: SiteConfig = field(repr=False)
    _group_config: GroupConfig = field(repr=False)
    _plugin_spec: PluginSpec = field(repr=False)

    def _build_host(self, provider: ProviderInterface) -> PluginHost:
        """Build one production host around this loaded plugin."""
        return PluginHost(
            self._site_config,
            self._group_config,
            provider,
            (self._plugin_spec,),
            heartbeat=lambda: None,
            shutdown_requested=lambda: False,
        )


class _HarnessProvider:
    """Return one caller-owned or offline handle without provider discovery."""

    def __init__(
        self,
        execution: Literal['sync', 'async'],
        handle: HarnessHandle,
    ) -> None:
        """Bind one handle to the requested execution mode."""
        for method_name, expected_async in (
            ('close', execution == 'async'),
            ('set_session_uuid', False),
        ):
            method = getattr(handle, method_name, None)
            if callable_mode(method) is not expected_async:
                mode = 'async' if expected_async else 'sync'
                raise ValueError(f'{execution} handle {method_name} must be {mode}')
        self.name = 'plugin-test-harness'
        self.capabilities = ProviderCapabilities(
            sync=execution == 'sync',
            async_=execution == 'async',
        )
        self._handle = handle

    async def create_async(
        self,
        site: str,
        credential: CredentialConfig,
    ) -> AsyncSG:
        """Return the injected asynchronous handle.

        Args:
            site: Ignored site URL.
            credential: Ignored resolved credential.

        Returns:
            The caller-injected or rejecting offline handle.
        """
        del site, credential
        return cast('AsyncSG', self._handle)

    def create_sync(
        self,
        site: str,
        credential: CredentialConfig,
    ) -> SyncSG:
        """Return the injected synchronous handle.

        Args:
            site: Ignored site URL.
            credential: Ignored resolved credential.

        Returns:
            The caller-injected or rejecting offline handle.
        """
        del site, credential
        return cast('SyncSG', self._handle)


class _OfflineAsyncHandle:
    """Reject SG requests while supporting the hosted async lifecycle."""

    def __getattr__(self, name: str) -> object:
        """Reject an unsupported SG operation when no handle was injected."""
        if name.startswith('_'):
            return object.__getattribute__(self, name)
        raise RuntimeError(_OFFLINE_GUIDANCE)

    async def close(self) -> None:
        """Release the no-op offline handle."""

    async def find(
        self,
        _entity_type: str,
        _filters: object,
        _fields: list[str] | None = None,
        **_kwargs: object,
    ) -> list[dict[str, object]]:
        """Reject an SG request when no async handle was injected.

        Args:
            _entity_type: Ignored requested entity type.
            _filters: Ignored requested filters.
            _fields: Ignored requested fields.
            **_kwargs: Ignored provider options.

        Raises:
            RuntimeError: Always, because the default harness is offline.
        """
        raise RuntimeError(_OFFLINE_GUIDANCE)

    def set_session_uuid(self, _session_uuid: str | None) -> None:
        """Accept session UUID changes without contacting SG.

        Args:
            _session_uuid: Ignored callback session UUID.
        """


class _OfflineSyncHandle:
    """Reject SG requests while supporting the hosted sync lifecycle."""

    def __getattr__(self, name: str) -> object:
        """Reject an unsupported SG operation when no handle was injected."""
        if name.startswith('_'):
            return object.__getattribute__(self, name)
        raise RuntimeError(_OFFLINE_GUIDANCE)

    def close(self) -> None:
        """Release the no-op offline handle."""

    def find(
        self,
        _entity_type: str,
        _filters: object,
        _fields: list[str] | None = None,
        **_kwargs: object,
    ) -> list[dict[str, object]]:
        """Reject an SG request when no sync handle was injected.

        Args:
            _entity_type: Ignored requested entity type.
            _filters: Ignored requested filters.
            _fields: Ignored requested fields.
            **_kwargs: Ignored provider options.

        Raises:
            RuntimeError: Always, because the default harness is offline.
        """
        raise RuntimeError(_OFFLINE_GUIDANCE)

    def set_session_uuid(self, _session_uuid: str | None) -> None:
        """Accept session UUID changes without contacting SG.

        Args:
            _session_uuid: Ignored callback session UUID.
        """


def _event(value: Event | Mapping[str, object]) -> Event:
    """Return one immutable synthetic event."""
    return value if isinstance(value, Event) else Event(value)


def _event_id(event: Event) -> int:
    """Validate the id of a synthetic event before invoking callbacks."""
    event_id = event['id']
    if type(event_id) is not int or event_id <= 0:
        raise ValueError('synthetic event id must be a positive integer')
    return event_id


def _failure_report(error: PluginError) -> FailureReport:
    """Copy one structured redacted plugin error into a frozen record."""
    return FailureReport(
        code=error.code,
        detail=str(error),
        site=error.site,
        group=error.group,
        plugin_id=error.plugin_id,
        callback_id=error.callback_id,
    )


def _callback_result(
    callback: CallbackSpec,
    event_id: int,
    delivery_attempt: int,
    failure: PluginError | None,
) -> CallbackResult:
    """Build one immutable callback result."""
    return CallbackResult(
        event_id=event_id,
        plugin_id=callback.identity.plugin_id,
        callback_id=callback.id,
        delivery_attempt=delivery_attempt,
        failure=None if failure is None else _failure_report(failure),
    )


def _provider(
    execution: Literal['sync', 'async'],
    handle: HarnessHandle | None,
) -> ProviderInterface:
    """Build the exact no-discovery harness provider."""
    if handle is None:
        handle = _OfflineSyncHandle() if execution == 'sync' else _OfflineAsyncHandle()
    return _HarnessProvider(execution, handle)


def _report_teardown_error(
    primary_error: BaseException | None, teardown_error: PluginError
) -> None:
    """Report a close failure without misattributing a caller's exception."""
    if primary_error is None:
        raise teardown_error
    primary_error.add_note(f'Plugin teardown also failed: {teardown_error}')


async def drive_async_plugin(
    plugin: LoadedPlugin,
    events: Iterable[Event | Mapping[str, object]],
    *,
    sg: HarnessHandle | None = None,
    delivery_attempt: int = 1,
    is_operator_replay: bool = False,
) -> DriveResult:
    """Run async setup and matching callbacks without a daemon or SG poller.

    Args:
        plugin: Plugin returned by `load_plugin` in async mode.
        events: Synthetic EventLogEntry mappings in delivery order.
        sg: Optional injected async provider handle. Passing it transfers
            ownership to the harness once setup acquires it. The harness closes
            it after success, an empty event sequence, or setup or callback
            failure. The offline default rejects every SG request.
        delivery_attempt: One-based attempt exposed to every callback context.
        is_operator_replay: Whether contexts identify the run as an operator
            replay.

    Returns:
        Immutable setup and callback results. Callback execution stops at the
        first failure, matching the group failure domain.

    Raises:
        PluginError: Closing an acquired handle has an ordinary teardown
            failure without another raised error. It has code
            `teardown-failed`; when a synthetic event is invalid, the teardown
            failure is noted on the `ValueError` instead.
        ValueError: The plugin or injected handle mode or delivery attempt is
            invalid, an event omits a required field, a matching event has an
            `id` that is not a positive integer, or an event field contains an
            unsupported or non-finite value.
    """
    if plugin.execution != 'async':
        raise ValueError('drive_async_plugin requires an async plugin')
    if delivery_attempt <= 0:
        raise ValueError('delivery_attempt must be positive')
    host = plugin._build_host(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        _provider('async', sg)
    )
    callbacks: list[CallbackResult] = []
    primary_error: BaseException | None = None
    try:
        setup_failure = await host.setup_async()
        if setup_failure is not None:
            return DriveResult(_failure_report(setup_failure), ())
        for event_value in events:
            event = _event(event_value)
            matching = host.matching_callbacks(event)
            if not matching:
                continue
            event_id = _event_id(event)
            for owner, callback in matching:
                failure = await host.invoke_async(
                    owner,
                    callback,
                    event,
                    delivery_attempt=delivery_attempt,
                    is_operator_replay=is_operator_replay,
                )
                callbacks.append(_callback_result(callback, event_id, delivery_attempt, failure))
                if failure is not None:
                    return DriveResult(None, tuple(callbacks))
    except BaseException as error:
        primary_error = error
        raise
    finally:
        try:
            await host.close_async()
        except PluginError as teardown_error:
            _report_teardown_error(primary_error, teardown_error)
    return DriveResult(None, tuple(callbacks))


def drive_sync_plugin(
    plugin: LoadedPlugin,
    events: Iterable[Event | Mapping[str, object]],
    *,
    sg: HarnessHandle | None = None,
    delivery_attempt: int = 1,
    is_operator_replay: bool = False,
) -> DriveResult:
    """Run sync setup and matching callbacks without a daemon or SG poller.

    Args:
        plugin: Plugin returned by `load_plugin` in sync mode.
        events: Synthetic EventLogEntry mappings in delivery order.
        sg: Optional injected sync provider handle. Passing it transfers
            ownership to the harness once setup acquires it. The harness closes
            it after success, an empty event sequence, or setup or callback
            failure. The offline default rejects every SG request.
        delivery_attempt: One-based attempt exposed to every callback context.
        is_operator_replay: Whether contexts identify the run as an operator
            replay.

    Returns:
        Immutable setup and callback results. Callback execution stops at the
        first failure, matching the group failure domain.

    Raises:
        PluginError: Closing an acquired handle has an ordinary teardown
            failure without another raised error. It has code
            `teardown-failed`; when a synthetic event is invalid, the teardown
            failure is noted on the `ValueError` instead.
        ValueError: The plugin or injected handle mode or delivery attempt is
            invalid, an event omits a required field, a matching event has an
            `id` that is not a positive integer, or an event field contains an
            unsupported or non-finite value.
    """
    if plugin.execution != 'sync':
        raise ValueError('drive_sync_plugin requires a sync plugin')
    if delivery_attempt <= 0:
        raise ValueError('delivery_attempt must be positive')
    host = plugin._build_host(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        _provider('sync', sg)
    )
    callbacks: list[CallbackResult] = []
    primary_error: BaseException | None = None
    try:
        setup_failure = host.setup_sync()
        if setup_failure is not None:
            return DriveResult(_failure_report(setup_failure), ())
        for event_value in events:
            event = _event(event_value)
            matching = host.matching_callbacks(event)
            if not matching:
                continue
            event_id = _event_id(event)
            for owner, callback in matching:
                failure = host.invoke_sync(
                    owner,
                    callback,
                    event,
                    delivery_attempt=delivery_attempt,
                    is_operator_replay=is_operator_replay,
                )
                callbacks.append(_callback_result(callback, event_id, delivery_attempt, failure))
                if failure is not None:
                    return DriveResult(None, tuple(callbacks))
    except BaseException as error:
        primary_error = error
        raise
    finally:
        try:
            host.close_sync()
        except PluginError as teardown_error:
            _report_teardown_error(primary_error, teardown_error)
    return DriveResult(None, tuple(callbacks))


def load_plugin(
    path: str | Path,
    *,
    plugin_id: str,
    execution: Literal['sync', 'async'] = 'sync',
    settings: HarnessSettings | None = None,
) -> LoadedPlugin:
    """Load one explicitly identified trusted plugin without starting a daemon.

    Importing the configured path executes trusted top-level Python. This
    helper is not a sandbox and does not promise side-effect-free validation.

    Args:
        path: Regular Python file containing a synchronous `register` function.
        plugin_id: Explicit stable plugin id.
        execution: Required setup and callback execution mode.
        settings: Optional context names, arbitrary non-secret JSON-compatible
            config, and environment-backed secret references.

    Returns:
        One immutable loaded plugin reusable across harness drives.

    Raises:
        PluginError: Configuration, import, registration, or mode validation
            fails.
    """
    credential = CredentialConfig(
        name=_HARNESS_CREDENTIAL,
        script_name='plugin-test-harness',
        api_key=SecretValue(_HARNESS_KEY),
    )
    resolved = HarnessSettings() if settings is None else settings
    plugin_config = PluginConfig(
        id=plugin_id,
        path=Path(path),
        credentials=_HARNESS_CREDENTIAL,
        config=resolved.config,
        secrets=MappingProxyType(
            {key: SecretRef(environment) for key, environment in resolved.secrets.items()}
        ),
    )
    group_config = GroupConfig(
        name=resolved.group,
        source=resolved.source,
        plugins=(plugin_config,),
        callback_credentials=_HARNESS_CREDENTIAL,
        execution=execution,
    )
    site_config = SiteConfig(
        name=resolved.site,
        server=_HARNESS_SERVER,
        default_source=resolved.source,
        credentials=MappingProxyType({_HARNESS_CREDENTIAL: credential}),
        sources=MappingProxyType({}),
        groups=MappingProxyType({resolved.group: group_config}),
        workers=MappingProxyType({}),
    )
    loaded = load_plugin_spec(site_config, group_config, plugin_config, 0)
    module = sys.modules[loaded.module_name]
    return LoadedPlugin(
        plugin_id=plugin_id,
        execution=execution,
        site=resolved.site,
        source=resolved.source,
        group=resolved.group,
        callbacks=tuple(callback.id for callback in loaded.callbacks),
        config=loaded.config,
        module=module,
        _site_config=site_config,
        _group_config=group_config,
        _plugin_spec=loaded,
    )
