"""Explicit trusted-plugin loading, registration, and hosted callback contexts."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import inspect
import logging
import math
import os
import re
import sys
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from importlib.util import module_from_spec, spec_from_file_location
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, TypeVar, cast, overload

from . import _logging as logging_support
from ._config import (
    GroupConfig,
    PluginConfig,
    SecretValue,
    SiteConfig,
)
from ._events import Event, EventFilter, matches, normalize_filter
from ._identity import canonical_bytes, digest

if TYPE_CHECKING:
    from pathlib import Path
    from types import ModuleType

    from ._config import CredentialConfig
    from ._providers import AsyncSG, ProviderInterface, SyncSG

_CALLBACK_NAME_PATTERN = re.compile(r'[a-z][a-z0-9_-]{0,63}\Z')
_EXCEPTION_FORMATTER = logging.Formatter()
_MODULE_ROOT = 'sg_event_daemon._trusted_plugins'
_SECRET_FILTER_LOCK = threading.Lock()
_SECRET_FILTER_STATE: list[_SecretFilter] = []
_ContextT = TypeVar('_ContextT', bound='SetupContext')
_TRUSTED_CODE_NOTICE = (
    'importing trusted plugin code executes top-level Python; it is not sandboxed '
    'or guaranteed side-effect-free'
)
_UNPRINTABLE = '<unprintable>'


@dataclass(frozen=True)
class CallbackIdentity:
    """Define one immutable durable callback identity."""

    ordinal: int
    plugin_id: str
    callback_id: str
    execution: Literal['sync', 'async']
    filter_digest: str
    config_digest: str
    secret_refs: tuple[tuple[str, str], ...]
    credential_name: str
    credential_fingerprint: str


@dataclass(frozen=True)
class CallbackManifest:
    """Contain the ordered immutable callback identities for one group."""

    callbacks: tuple[CallbackIdentity, ...]

    def to_json(self) -> str:
        """Return canonical compact non-secret manifest JSON.

        Returns:
            Canonical JSON containing only durable non-secret callback
            identity.
        """
        payload = {
            'callbacks': [
                {
                    'callback_id': callback.callback_id,
                    'config_digest': callback.config_digest,
                    'credential_fingerprint': callback.credential_fingerprint,
                    'credential_name': callback.credential_name,
                    'execution': callback.execution,
                    'filter_digest': callback.filter_digest,
                    'ordinal': callback.ordinal,
                    'plugin_id': callback.plugin_id,
                    'secret_refs': [list(reference) for reference in callback.secret_refs],
                }
                for callback in self.callbacks
            ]
        }
        return canonical_bytes(payload).decode()


@dataclass(frozen=True)
class SetupContext:
    """Expose the fixed public context for one plugin setup invocation.

    `sg` is borrowed from the host and must not be closed by the plugin.
    `config` is deeply immutable non-secret configuration; `secrets` maps
    configured names to redacted values revealed explicitly with `reveal()`.
    The host supplies the plugin logger, configured identities, heartbeat, and
    cooperative shutdown state.
    """

    sg: SyncSG | AsyncSG
    logger: logging.Logger
    config: Mapping[str, object]
    secrets: Mapping[str, SecretValue]
    site: str
    source: str
    group: str
    plugin_id: str
    _heartbeat_callback: Callable[[], None] = field(repr=False)
    _shutdown_check: Callable[[], bool] = field(repr=False)

    def heartbeat(self) -> None:
        """Refresh the owning lane's callback watchdog."""
        self._heartbeat_callback()

    @property
    def shutdown_requested(self) -> bool:
        """Return the host's current cooperative shutdown state.

        Returns:
            Whether cooperative shutdown is currently requested.
        """
        return self._shutdown_check()


@dataclass(frozen=True)
class EventContext(SetupContext):
    """Expose fixed setup and delivery fields for one callback invocation.

    The inherited handle and configuration follow `SetupContext` ownership.
    `event` is immutable, `callback_id` identifies the registered callback,
    `delivery_attempt` is one-based. `is_operator_replay` is true for
    deliveries while an operator replay is catching up, until the group rejoins
    live delivery.

    Complete all callback work before returning, including any spawned work.
    Effects must tolerate repeated delivery: a callback can run again when its
    effects occurred before durable completion was recorded.
    """

    event: Event
    callback_id: str
    delivery_attempt: int
    is_operator_replay: bool


@dataclass(frozen=True)
class CallbackSpec:
    """Bind one registered callback to its filter and durable identity."""

    id: str
    callback: Callable[[EventContext], object] = field(repr=False)
    events: EventFilter | None
    identity: CallbackIdentity


@dataclass(frozen=True)
class PluginSpec:
    """Contain one loaded plugin's immutable registration result."""

    id: str
    module_name: str
    credentials: str
    config: Mapping[str, object]
    secrets: tuple[tuple[str, str], ...]
    setup: Callable[[SetupContext], object] | None = field(repr=False)
    callbacks: tuple[CallbackSpec, ...]


@dataclass(frozen=True)
class _IdentitySeed:
    """Carry shared durable identity fields for one plugin."""

    _plugin_id: str
    _execution: Literal['sync', 'async']
    _config_digest: str
    _secret_refs: tuple[tuple[str, str], ...]
    _credential_name: str
    _credential_fingerprint: str
    _starting_ordinal: int


@dataclass(frozen=True)
class _PluginLocation:
    """Identify one plugin failure location."""

    _site: str
    _group: str
    _plugin_id: str | None = None
    _callback_id: str | None = None


class PluginError(RuntimeError):
    """Report a structured plugin registration or group-halt failure.

    Attributes:
        code: Stable machine-readable failure classification.
        site: Site identity associated with the failure.
        group: Group identity associated with the failure.
        plugin_id: Plugin identity, or `None` for a group-level failure.
        callback_id: Callback identity, or `None` when no callback is
            identified.
    """

    __slots__ = ('callback_id', 'code', 'group', 'plugin_id', 'site')

    def __init__(
        self,
        code: str,
        detail: str,
        location: _PluginLocation,
    ) -> None:
        """Record one redacted failure with stable callback identity.

        Args:
            code: Stable machine-readable failure classification.
            detail: Human-readable failure detail.
            location: Stable site, group, plugin, and callback identity.
        """
        self.callback_id = location._callback_id
        self.code = code
        self.group = location._group
        self.plugin_id = location._plugin_id
        self.site = location._site
        identity = f'site {location._site!r}, group {location._group!r}'
        if location._plugin_id is not None:
            identity += f', plugin {location._plugin_id!r}'
        if location._callback_id is not None:
            identity += f', callback {location._callback_id!r}'
        message = f'{code} for {identity}: {detail}; {_TRUSTED_CODE_NOTICE}'
        super().__init__(_redact_text(message))


class PluginHost:
    """Create plugin handles and fixed public contexts in one group lane.

    Run setup in the group's execution mode before invoking callbacks.
    The host owns and reuses provider handles across invocations, including
    handles opened before a setup failure. Release them with the matching
    synchronous or asynchronous close operation.

    Contexts borrow the host's handles and expose frozen configuration and
    explicit secret wrappers.
    """

    def __init__(  # noqa: PLR0913
        self,
        site: SiteConfig,
        group: GroupConfig,
        provider: ProviderInterface,
        plugins: tuple[PluginSpec, ...],
        *,
        heartbeat: Callable[[], None],
        shutdown_requested: Callable[[], bool],
    ) -> None:
        """Create one unstarted plugin host.

        Args:
            site: Resolved site configuration and credentials.
            group: Resolved group configuration.
            provider: Validated SG provider factory.
            plugins: Ordered explicit plugin registrations.
            heartbeat: Lane heartbeat callback.
            shutdown_requested: Dynamic cooperative shutdown callback.
        """
        self._group = group
        self._handles: dict[str, SyncSG | AsyncSG] = {}
        self._heartbeat = heartbeat
        self._plugins = plugins
        self._provider = provider
        self._resolved_secrets: dict[str, Mapping[str, SecretValue]] = {}
        self._shutdown_requested = shutdown_requested
        self._site = site

    def _credential(self, plugin: PluginSpec) -> CredentialConfig:
        """Return and register one plugin's configured callback credential."""
        credential = self._site.credentials[plugin.credentials]
        _register_secret(credential.api_key.reveal())
        return credential

    def _failure(
        self,
        code: str,
        error: object,
        plugin: PluginSpec,
        callback: CallbackSpec | None = None,
    ) -> PluginError:
        """Build one redacted structured group halt."""
        return PluginError(
            code,
            str(error),
            _plugin_location(
                self._site,
                self._group,
                plugin.id,
                None if callback is None else callback.id,
            ),
        )

    def _logger(
        self,
        plugin: PluginSpec,
        callback: CallbackSpec | None = None,
    ) -> logging.Logger:
        """Return one plugin- or callback-scoped redacting logger."""
        name = f'sg_event_daemon.plugin.{self._site.name}.{self._group.name}.{plugin.id}'
        if callback is not None:
            name = f'{name}.{callback.id}'
        logger = logging.getLogger(name)
        secret_filter = _secret_filter()
        with _SECRET_FILTER_LOCK:
            if not any(existing is secret_filter for existing in logger.filters):
                logger.filters.append(secret_filter)
        return logger

    def _callback_context(
        self,
        plugin: PluginSpec,
        callback: CallbackSpec,
        event: Event,
        delivery_attempt: int,
        is_operator_replay: bool,
    ) -> EventContext:
        """Build one fixed callback context."""
        return EventContext(
            sg=self._handles[plugin.id],
            logger=self._logger(plugin, callback),
            config=plugin.config,
            secrets=self._resolved_secrets[plugin.id],
            site=self._site.name,
            source=self._group.source,
            group=self._group.name,
            plugin_id=plugin.id,
            _heartbeat_callback=self._heartbeat,
            _shutdown_check=self._shutdown_requested,
            event=event,
            callback_id=callback.id,
            delivery_attempt=delivery_attempt,
            is_operator_replay=is_operator_replay,
        )

    def _prepare_secrets(self) -> PluginError | None:
        """Resolve and register all hosted secrets before setup starts."""
        resolved: dict[str, Mapping[str, SecretValue]] = {}
        for plugin in self._plugins:
            self._credential(plugin)
            values: dict[str, SecretValue] = {}
            for key, environment_name in plugin.secrets:
                try:
                    value = os.environ[environment_name]
                except KeyError:
                    return self._failure(
                        'secret-missing',
                        f'environment variable {environment_name!r} is not set',
                        plugin,
                    )
                _register_secret(value)
                values[key] = SecretValue(value)
            resolved[plugin.id] = MappingProxyType(values)
        self._resolved_secrets = resolved
        return None

    def _setup_context(self, plugin: PluginSpec) -> SetupContext:
        """Build one fixed setup context."""
        return SetupContext(
            sg=self._handles[plugin.id],
            logger=self._logger(plugin),
            config=plugin.config,
            secrets=self._resolved_secrets[plugin.id],
            site=self._site.name,
            source=self._group.source,
            group=self._group.name,
            plugin_id=plugin.id,
            _heartbeat_callback=self._heartbeat,
            _shutdown_check=self._shutdown_requested,
        )

    def _teardown_failure(
        self,
        error: BaseException,
        plugin: PluginSpec,
    ) -> BaseException:
        """Redact one ordinary plugin-handle teardown failure."""
        if not isinstance(error, Exception):
            return error
        failure = self._failure('teardown-failed', _redacted_string(error), plugin)
        failure.__cause__ = error
        return failure

    async def close_async(self) -> None:
        """Close every acquired asynchronous handle and relinquish ownership.

        Cleanup attempts every acquired handle in reverse configured order and
        clears host ownership before raising the first failure observed in that
        order.

        Raises:
            PluginError: The first failure is an ordinary exception, redacted
                with code `teardown-failed`.
            BaseException: The first failure is not an ordinary exception and
                is re-raised unchanged.
        """
        first_failure: tuple[BaseException, PluginSpec] | None = None
        try:
            for plugin in reversed(self._plugins):
                handle = self._handles.get(plugin.id)
                if handle is None:
                    continue
                try:
                    await cast('AsyncSG', handle).close()
                except BaseException as error:  # noqa: BLE001
                    if first_failure is None:
                        first_failure = (error, plugin)
        finally:
            self._handles.clear()
        if first_failure is not None:
            error, plugin = first_failure
            raise self._teardown_failure(error, plugin)

    def close_sync(self) -> None:
        """Close every acquired synchronous handle and relinquish ownership.

        Cleanup attempts every acquired handle in reverse configured order and
        clears host ownership before raising the first failure observed in that
        order.

        Raises:
            PluginError: The first failure is an ordinary exception, redacted
                with code `teardown-failed`.
            BaseException: The first failure is not an ordinary exception and
                is re-raised unchanged.
        """
        first_failure: tuple[BaseException, PluginSpec] | None = None
        try:
            for plugin in reversed(self._plugins):
                handle = self._handles.get(plugin.id)
                if handle is None:
                    continue
                try:
                    cast('SyncSG', handle).close()
                except BaseException as error:  # noqa: BLE001
                    if first_failure is None:
                        first_failure = (error, plugin)
        finally:
            self._handles.clear()
        if first_failure is not None:
            error, plugin = first_failure
            raise self._teardown_failure(error, plugin)

    async def invoke_async(
        self,
        plugin: PluginSpec,
        callback: CallbackSpec,
        event: Event,
        *,
        delivery_attempt: int,
        is_operator_replay: bool,
    ) -> PluginError | None:
        """Invoke one async callback after applying its event session UUID.

        Args:
            plugin: Owning loaded plugin.
            callback: Registered callback to invoke.
            event: Immutable source-visible event.
            delivery_attempt: Durable one-based invocation attempt.
            is_operator_replay: Whether delivery belongs to an operator replay.

        Returns:
            A structured group halt on failure, otherwise `None`.
        """
        handle = self._handles.get(plugin.id)
        if handle is None:
            return self._failure('host-not-started', 'plugin setup has not run', plugin, callback)
        session_uuid = event['session_uuid']
        if session_uuid is not None and not isinstance(session_uuid, str):
            return self._failure(
                'event-invalid',
                'event session_uuid must be a string or null',
                plugin,
                callback,
            )
        try:
            cast('AsyncSG', handle).set_session_uuid(session_uuid)
            context = self._callback_context(
                plugin,
                callback,
                event,
                delivery_attempt,
                is_operator_replay,
            )
            result = callback.callback(context)
            if not inspect.isawaitable(result):
                return self._failure(
                    'callback-failed',
                    f'async callback {callback.id!r} did not return an awaitable',
                    plugin,
                    callback,
                )
            await result
        except Exception as error:  # noqa: BLE001
            return self._failure('callback-failed', error, plugin, callback)
        return None

    def invoke_sync(
        self,
        plugin: PluginSpec,
        callback: CallbackSpec,
        event: Event,
        *,
        delivery_attempt: int,
        is_operator_replay: bool,
    ) -> PluginError | None:
        """Invoke one sync callback after applying its event session UUID.

        Args:
            plugin: Owning loaded plugin.
            callback: Registered callback to invoke.
            event: Immutable source-visible event.
            delivery_attempt: Durable one-based invocation attempt.
            is_operator_replay: Whether delivery belongs to an operator replay.

        Returns:
            A structured group halt on failure, otherwise `None`.
        """
        handle = self._handles.get(plugin.id)
        if handle is None:
            return self._failure('host-not-started', 'plugin setup has not run', plugin, callback)
        session_uuid = event['session_uuid']
        if session_uuid is not None and not isinstance(session_uuid, str):
            return self._failure(
                'event-invalid',
                'event session_uuid must be a string or null',
                plugin,
                callback,
            )
        try:
            cast('SyncSG', handle).set_session_uuid(session_uuid)
            context = self._callback_context(
                plugin,
                callback,
                event,
                delivery_attempt,
                is_operator_replay,
            )
            result = callback.callback(context)
            validate_sync_callback_result(result, callback.id)
        except Exception as error:  # noqa: BLE001
            return self._failure('callback-failed', error, plugin, callback)
        return None

    def matching_callbacks(
        self,
        event: Event,
    ) -> tuple[tuple[PluginSpec, CallbackSpec], ...]:
        """Return registered callbacks matching one event in manifest order.

        Args:
            event: Immutable source-visible event.

        Returns:
            Ordered plugin and callback pairs whose normalized filters match.
        """
        return tuple(
            (plugin, callback)
            for plugin in self._plugins
            for callback in plugin.callbacks
            if matches(callback.events, event)
        )

    async def setup_async(self) -> PluginError | None:
        """Create async handles and await ordered plugin setup functions.

        Repeated calls reuse existing handles and restart ordered setup from
        the first plugin. If setup fails, the host retains all acquired handles
        for retry or explicit cleanup.

        Returns:
            A structured group halt on the first failure, otherwise `None`.
        """
        secret_failure = self._prepare_secrets()
        if secret_failure is not None:
            return secret_failure
        for plugin in self._plugins:
            try:
                if plugin.id not in self._handles:
                    self._handles[plugin.id] = await self._provider.create_async(
                        self._site.server,
                        self._credential(plugin),
                    )
                if plugin.setup is not None:
                    result = plugin.setup(self._setup_context(plugin))
                    if not inspect.isawaitable(result):
                        return self._failure(
                            'setup-failed',
                            'async setup did not return an awaitable',
                            plugin,
                        )
                    await result
            except asyncio.CancelledError as error:
                return self._failure('setup-failed', error, plugin)
            except Exception as error:  # noqa: BLE001
                return self._failure('setup-failed', error, plugin)
        return None

    def setup_sync(self) -> PluginError | None:
        """Create sync handles and run ordered plugin setup functions.

        Repeated calls reuse existing handles and restart ordered setup from
        the first plugin. If setup fails, the host retains all acquired handles
        for retry or explicit cleanup.

        Returns:
            A structured group halt on the first failure, otherwise `None`.
        """
        secret_failure = self._prepare_secrets()
        if secret_failure is not None:
            return secret_failure
        for plugin in self._plugins:
            try:
                if plugin.id not in self._handles:
                    self._handles[plugin.id] = self._provider.create_sync(
                        self._site.server,
                        self._credential(plugin),
                    )
                if plugin.setup is not None:
                    plugin.setup(self._setup_context(plugin))
            except Exception as error:  # noqa: BLE001
                return self._failure('setup-failed', error, plugin)
        return None


@dataclass(frozen=True)
class _RegisteredCallback:
    """Hold callback registration before durable identity construction."""

    _id: str
    _callback: Callable[[EventContext], object] = field(repr=False)
    _events: EventFilter | None


@dataclass
class _RegistrationState:
    """Hold mutable registration state shared with the loader."""

    _callbacks: list[_RegisteredCallback]
    _frozen: bool
    _setup: Callable[[SetupContext], object] | None
    _setup_was_set: bool


class _Registrar:
    """Collect one trusted plugin's immutable explicit registration."""

    __slots__ = ('_config', '_location', '_state')

    def __init__(
        self,
        config: Mapping[str, object],
        location: _PluginLocation,
        state: _RegistrationState,
    ) -> None:
        """Create one open registration collector."""
        self._config = config
        self._location = location
        self._state = state

    def _check_open(self) -> None:
        """Reject mutation after the registration function returns."""
        if self._state._frozen:
            raise PluginError(
                'registration-frozen',
                'registration is frozen after register returns',
                self._location,
            )

    def add_callback(
        self,
        *,
        callback: object,
        events: object | None = None,
        **values: object,
    ) -> None:
        """Register one stable callback and optional event filter.

        Args:
            callback: Callback accepting one `EventContext` and compatible with
                the group's execution mode.
            events: Mapping of nonempty event-type names to `None` or finite
                collections of nonempty attribute names. Strings, bytes, and
                mappings are not attribute collections. `None` or an empty
                mapping matches every event. `*` matches every event type or
                attribute; `None` matches every attribute of an event type,
                while an empty collection matches none.
            **values: Exactly one `id` field, unique within this plugin and
                matching `[a-z][a-z0-9_-]{0,63}`.

        Raises:
            PluginError: Registration is frozen or the declaration is invalid.
        """
        self._check_open()
        callback_id = values.pop('id', None)
        if values:
            unknown = ', '.join(sorted(values))
            raise PluginError(
                'registration-invalid',
                f'unknown callback declaration fields: {unknown}',
                self._location,
            )
        if (
            not isinstance(callback_id, str)
            or _CALLBACK_NAME_PATTERN.fullmatch(callback_id) is None
        ):
            raise PluginError(
                'registration-invalid',
                f'invalid callback id {callback_id!r}',
                self._location,
            )
        callback_location = _PluginLocation(
            _site=self._location._site,
            _group=self._location._group,
            _plugin_id=self._location._plugin_id,
            _callback_id=callback_id,
        )
        if any(existing._id == callback_id for existing in self._state._callbacks):
            raise PluginError(
                'registration-invalid',
                f'duplicate callback id {callback_id!r}',
                callback_location,
            )
        if not callable(callback):
            raise PluginError(
                'registration-invalid',
                f'callback {callback_id!r} is not callable',
                callback_location,
            )
        try:
            normalized = normalize_filter(events)
        except (TypeError, ValueError) as error:
            raise PluginError(
                'registration-invalid',
                f'callback {callback_id!r} has an invalid filter: {error}',
                callback_location,
            ) from None
        self._state._callbacks.append(
            _RegisteredCallback(
                _id=callback_id,
                _callback=cast('Callable[[EventContext], object]', callback),
                _events=normalized,
            )
        )

    @property
    def config(self) -> Mapping[str, object]:
        """Return deeply immutable non-secret plugin configuration.

        Returns:
            Deeply immutable non-secret plugin configuration.
        """
        return self._config

    def set_setup(self, setup: object) -> None:
        """Register the plugin's optional idempotent setup function.

        Args:
            setup: One-argument setup-mode-compatible callable.

        Raises:
            PluginError: Registration is frozen, setup is not callable, or
                setup was already declared.
        """
        self._check_open()
        if self._state._setup_was_set:
            raise PluginError(
                'registration-invalid',
                'setup was registered more than once',
                self._location,
            )
        if not callable(setup):
            raise PluginError(
                'registration-invalid',
                'setup is not callable',
                self._location,
            )
        self._state._setup = cast('Callable[[SetupContext], object]', setup)
        self._state._setup_was_set = True


class _SecretFilter(logging.Filter):
    """Redact registered values recursively before record propagation."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact the mutable logging record.

        Args:
            record: Record emitted through a hosted plugin logger.

        Returns:
            Always `True` so the redacted record continues to handlers.
        """
        for name, value in tuple(record.__dict__.items()):
            if name != 'exc_info':
                record.__dict__[name] = (
                    _redact_args(record.args) if name == 'args' else _redact_value(value)
                )
        if record.exc_info is not None:
            exception_type, exception, _traceback = record.exc_info
            if exception_type is None or exception is None:
                record.exc_text = None
                record.exc_info = None
            else:
                record.exc_text = _redact_text(
                    _EXCEPTION_FORMATTER.formatException(record.exc_info)
                )
                record.exc_info = None
        record.msg = _redact_text(record.getMessage())
        record.args = ()
        return True


def _callable_is_async(value: Callable[..., object]) -> bool:
    """Classify an unwrapped function or callable target."""
    targets = (inspect.unwrap(value), inspect.unwrap(value.__call__))
    return any(inspect.iscoroutinefunction(target) for target in targets)


def _freeze_json(value: object) -> object:
    """Copy and recursively freeze one JSON-compatible value."""
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError('plugin config contains a non-finite number')
        return value
    if isinstance(value, Mapping):
        raw = cast('Mapping[object, object]', value)
        if not all(isinstance(key, str) for key in raw):
            raise TypeError('plugin config contains a non-string object key')
        return MappingProxyType(
            {key: _freeze_json(item) for key, item in cast('Mapping[str, object]', raw).items()}
        )
    if isinstance(value, tuple | list):
        return tuple(
            _freeze_json(item) for item in cast('tuple[object, ...] | list[object]', value)
        )
    raise TypeError(f'plugin config contains unsupported {type(value).__name__}')


def _plugin_location(
    site: SiteConfig,
    group: GroupConfig,
    plugin_id: str | None = None,
    callback_id: str | None = None,
) -> _PluginLocation:
    """Build one stable failure location from resolved configuration."""
    return _PluginLocation(
        _site=site.name,
        _group=group.name,
        _plugin_id=plugin_id,
        _callback_id=callback_id,
    )


def _check_mode(
    setup: Callable[[SetupContext], object] | None,
    callbacks: tuple[_RegisteredCallback, ...],
    site: SiteConfig,
    group: GroupConfig,
    plugin_id: str,
) -> None:
    """Reject native coroutine mode mismatches before setup can run."""
    expects_async = group.execution == 'async'
    if setup is not None:
        setup_is_async = _callable_is_async(setup)
        if setup_is_async != expects_async:
            actual = 'async' if setup_is_async else 'ordinary'
            raise PluginError(
                'registration-invalid',
                f'{group.execution} group rejects {actual} setup',
                _plugin_location(site, group, plugin_id),
            )
    for callback in callbacks:
        callback_is_async = _callable_is_async(callback._callback)
        if callback_is_async != expects_async:
            actual = 'async' if callback_is_async else 'ordinary'
            raise PluginError(
                'registration-invalid',
                f'{group.execution} group rejects {actual} callback {callback._id!r}',
                _plugin_location(site, group, plugin_id, callback._id),
            )


def _check_provider(site: SiteConfig, group: GroupConfig, provider: ProviderInterface) -> None:
    """Reject a provider that cannot host the configured execution mode."""
    if group.execution == 'async' and not provider.capabilities.async_:
        raise PluginError(
            'provider-incompatible',
            f'provider {provider.name!r} does not support async plugin handles',
            _plugin_location(site, group),
        )
    if group.execution == 'sync' and not provider.capabilities.sync:
        raise PluginError(
            'provider-incompatible',
            f'provider {provider.name!r} does not support sync plugin handles',
            _plugin_location(site, group),
        )


def _credential_fingerprint(site: SiteConfig, plugin: PluginConfig, group: GroupConfig) -> str:
    """Return the non-secret effective callback-credential fingerprint."""
    try:
        credential = site.credentials[plugin.credentials]
    except KeyError:
        raise PluginError(
            'configuration-invalid',
            f'unknown plugin credentials {plugin.credentials!r}',
            _plugin_location(site, group, plugin.id),
        ) from None
    return digest(
        canonical_bytes(
            {
                'name': credential.name,
                'script_name': credential.script_name,
            }
        )
    )


def _freeze_plugin_config(
    site: SiteConfig,
    group: GroupConfig,
    plugin: PluginConfig,
) -> Mapping[str, object]:
    """Copy and freeze one configured plugin's non-secret data."""
    try:
        return cast('Mapping[str, object]', _freeze_json(plugin.config))
    except (TypeError, ValueError) as error:
        raise PluginError(
            'configuration-invalid',
            str(error),
            _plugin_location(site, group, plugin.id),
        ) from None


def _redact_text(value: str) -> str:
    """Replace every registered secret and digest in one string."""
    return logging_support.redact_text(value)


def _redacted_string(value: object) -> str:
    """Return a safely rendered and redacted logging value."""
    try:
        rendered = str(value)
    except Exception:  # noqa: BLE001
        return _UNPRINTABLE
    return _redact_text(rendered)


@overload
def _redact_scalar(value: str, /) -> str: ...


@overload
def _redact_scalar(value: object, /) -> object: ...


def _redact_scalar(value: object) -> object:
    """Redact one plugin-log scalar without changing numeric values."""
    if value is None or isinstance(value, int | float):
        return value
    return _redacted_string(value)


def _redact_value(value: object) -> object:
    """Recursively redact registered secrets from one logging value."""
    return logging_support.transform_log_value(value, _redact_scalar, mode='plugin')


def _redact_arg(value: object) -> object:
    """Redact one interpolation argument without stringifying opaque scalars."""
    if isinstance(value, str | bytes | Mapping | tuple | list | set | frozenset):
        return _redact_value(cast('object', value))
    return value


def _redact_args(
    args: tuple[object, ...] | Mapping[str, object] | None,
) -> tuple[object, ...] | Mapping[str, object] | None:
    """Selectively redact positional or named interpolation arguments."""
    if args is None:
        return None
    if isinstance(args, tuple):
        return tuple(_redact_arg(value) for value in args)
    return {name: _redact_arg(value) for name, value in args.items()}


def _register_secret(value: str) -> None:
    """Register one plaintext secret and its digest for redaction."""
    protected = {digest(value.encode())}
    if value:
        protected.add(value)
    logging_support.register_redaction_values(*(SecretValue(item) for item in protected))


def _register_environment_secrets(plugin: PluginConfig) -> None:
    """Register every currently available environment-backed plugin secret."""
    for reference in plugin.secrets.values():
        value = os.environ.get(reference.env)
        if value is not None:
            _register_secret(value)


def _register_plugin_secrets(site: SiteConfig, plugin: PluginConfig) -> None:
    """Register one plugin's available credential and environment secrets."""
    credential = site.credentials.get(plugin.credentials)
    if credential is not None:
        _register_secret(credential.api_key.reveal())
    _register_environment_secrets(plugin)


def _register_site_secrets(site: SiteConfig) -> None:
    """Register every available credential and configured site plugin secret."""
    for credential in site.credentials.values():
        _register_secret(credential.api_key.reveal())
    for group in site.groups.values():
        for plugin in group.plugins:
            _register_environment_secrets(plugin)


def _remove_module(module_name: str, module: object) -> None:
    """Remove one failed synthetic import without disturbing replacements."""
    if sys.modules.get(module_name) is module:
        del sys.modules[module_name]


def _load_module(
    path: Path,
    module_name: str,
    site: SiteConfig,
    group: GroupConfig,
    plugin_id: str,
) -> ModuleType:
    """Execute one explicitly configured trusted plugin module."""
    spec = spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise PluginError(
            'import-failed',
            f'cannot construct an import specification for {path}',
            _plugin_location(site, group, plugin_id),
        )
    module = module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as error:  # noqa: BLE001
        _remove_module(module_name, module)
        raise PluginError(
            'import-failed',
            f'plugin import failed: {error}',
            _plugin_location(site, group, plugin_id),
        ) from None
    return module


def _run_registration(
    module: object,
    config: Mapping[str, object],
    location: _PluginLocation,
) -> tuple[Callable[[SetupContext], object] | None, tuple[_RegisteredCallback, ...]]:
    """Run and freeze one module's explicit registration function."""
    register = getattr(module, 'register', None)
    if not callable(register):
        raise PluginError(
            'registration-invalid',
            'plugin is missing callable register',
            location,
        )
    if inspect.iscoroutinefunction(register):
        raise PluginError(
            'registration-invalid',
            'registration must be synchronous',
            location,
        )
    state = _RegistrationState(
        _callbacks=[],
        _frozen=False,
        _setup=None,
        _setup_was_set=False,
    )
    registrar = _Registrar(config, location, state)
    try:
        result = cast('Callable[[_Registrar], object]', register)(registrar)
    except PluginError:
        raise
    except Exception as error:  # noqa: BLE001
        raise PluginError(
            'registration-failed',
            f'registration failed: {error}',
            location,
        ) from None
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise PluginError(
            'registration-invalid',
            'registration must be synchronous and return a non-awaitable result',
            location,
        )
    state._frozen = True
    return state._setup, tuple(state._callbacks)


def _secret_filter() -> _SecretFilter:
    """Return the process-wide hosted logger filter."""
    with _SECRET_FILTER_LOCK:
        if _SECRET_FILTER_STATE:
            return _SECRET_FILTER_STATE[0]
        secret_filter = _SecretFilter()
        _SECRET_FILTER_STATE.append(secret_filter)
        return secret_filter


def _secret_refs(plugin: PluginConfig) -> tuple[tuple[str, str], ...]:
    """Return sorted symbolic secret references without resolving values."""
    references: list[tuple[str, str]] = []
    for key, reference in plugin.secrets.items():
        references.append((key, reference.env))
    return tuple(sorted(references))


def _thaw_json(value: object) -> object:
    """Convert one frozen JSON value into JSON encoder containers."""
    if isinstance(value, Mapping):
        mapping = cast('Mapping[str, object]', value)
        return {key: _thaw_json(item) for key, item in mapping.items()}
    if isinstance(value, tuple):
        items = cast('tuple[object, ...]', value)
        return [_thaw_json(item) for item in items]
    return value


def _callback_specs(
    callbacks: tuple[_RegisteredCallback, ...],
    seed: _IdentitySeed,
) -> tuple[CallbackSpec, ...]:
    """Build durable identities for one plugin's ordered callbacks."""
    return tuple(
        CallbackSpec(
            id=registered._id,
            callback=registered._callback,
            events=registered._events,
            identity=CallbackIdentity(
                ordinal=seed._starting_ordinal + offset,
                plugin_id=seed._plugin_id,
                callback_id=registered._id,
                execution=seed._execution,
                filter_digest=digest(canonical_bytes(_thaw_json(registered._events or ()))),
                config_digest=seed._config_digest,
                secret_refs=seed._secret_refs,
                credential_name=seed._credential_name,
                credential_fingerprint=seed._credential_fingerprint,
            ),
        )
        for offset, registered in enumerate(callbacks)
    )


def load_plugin_spec(
    site: SiteConfig,
    group: GroupConfig,
    plugin: PluginConfig,
    starting_ordinal: int,
) -> PluginSpec:
    """Load and freeze one configured plugin instance.

    Execute the trusted plugin module and its registration hook in the current
    process; loading is not a side-effect-free inspection.

    Args:
        site: Resolved site configuration used for identity and credentials.
        group: Resolved group configuration used for mode validation.
        plugin: Explicit plugin configuration to load.
        starting_ordinal: First durable callback ordinal for this plugin.

    Returns:
        The immutable registered plugin specification.

    Raises:
        PluginError: Configuration, import, registration, or execution mode is
            incompatible with the plugin contract.
    """
    _register_plugin_secrets(site, plugin)
    location = _plugin_location(site, group, plugin.id)
    if not plugin.path.is_file():
        raise PluginError(
            'configuration-invalid',
            f'plugin path must be a regular file: {plugin.path}',
            location,
        )
    config = _freeze_plugin_config(site, group, plugin)
    module_name = f'{_MODULE_ROOT}.{site.name}.{group.name}.{plugin.id}'
    module = _load_module(plugin.path, module_name, site, group, plugin.id)
    try:
        setup, registered_callbacks = _run_registration(module, config, location)
        _check_mode(setup, registered_callbacks, site, group, plugin.id)
        references = _secret_refs(plugin)
        callbacks = _callback_specs(
            registered_callbacks,
            _IdentitySeed(
                _plugin_id=plugin.id,
                _execution=group.execution,
                _config_digest=digest(canonical_bytes(_thaw_json(config))),
                _secret_refs=references,
                _credential_name=plugin.credentials,
                _credential_fingerprint=_credential_fingerprint(site, plugin, group),
                _starting_ordinal=starting_ordinal,
            ),
        )
    except PluginError:
        _remove_module(module_name, module)
        raise
    return PluginSpec(
        id=plugin.id,
        module_name=module_name,
        credentials=plugin.credentials,
        config=config,
        secrets=references,
        setup=setup,
        callbacks=callbacks,
    )


def load_plugins(
    site: SiteConfig,
    group: GroupConfig,
    provider: ProviderInterface,
) -> tuple[PluginSpec, ...]:
    """Import and freeze the group's explicitly configured trusted plugins.

    Importing each configured path executes trusted top-level Python. The
    loader is not a sandbox and does not promise side-effect-free validation.

    Args:
        site: Resolved site configuration used for namespaces and credentials.
        group: Ordered group and plugin configuration.
        provider: Validated provider used to confirm execution capability.

    Returns:
        Ordered immutable plugin registrations.

    Raises:
        PluginError: Configuration, import, registration, or execution mode is
            incompatible with the plugin contract.
    """
    _register_site_secrets(site)
    _check_provider(site, group, provider)
    plugin_ids: set[str] = set()
    plugins: list[PluginSpec] = []
    ordinal = 0
    for plugin in group.plugins:
        if plugin.id in plugin_ids:
            raise PluginError(
                'configuration-invalid',
                f'duplicate plugin id {plugin.id!r}',
                _plugin_location(site, group, plugin.id),
            )
        plugin_ids.add(plugin.id)
        loaded = load_plugin_spec(site, group, plugin, ordinal)
        plugins.append(loaded)
        ordinal += len(loaded.callbacks)
    return tuple(plugins)


def validate_sync_callback_result(result: object, callback_id: str) -> None:
    """Reject awaitable callback results, closing native coroutines first.

    Args:
        result: Actual return value from the synchronous callback.
        callback_id: Registered callback identity included in the failure.

    Raises:
        TypeError: The callback returned an awaitable instead of a sync result.
    """
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise TypeError(f'sync callback {callback_id!r} returned an awaitable')


def with_heartbeat(context: _ContextT, heartbeat: Callable[[], None]) -> _ContextT:
    """Copy a setup or event context with a new host heartbeat callback.

    Args:
        context: Existing immutable hosted callback context.
        heartbeat: Callback used by the copied context's `heartbeat` method.

    Returns:
        A context of the same concrete type with all other fields preserved.
    """
    return replace(context, _heartbeat_callback=heartbeat)
