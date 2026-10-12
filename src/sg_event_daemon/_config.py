"""Strict immutable configuration loading."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import ipaddress
import math
import os
import re
import tomllib
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Literal, TypedDict, cast

from ._event_frame import event_frame_bytes
from ._identity import canonical_bytes, stream_fingerprint
from ._providers import SUPPORTED_PROVIDERS
from ._state_paths import source_socket_kind
from ._watchdog import provider_budget_secs

DEFAULT_BATCH_SIZE = 500
DEFAULT_CHECKPOINT_INTERVAL_SECS = 1.0
DEFAULT_COMMIT_BATCH_MAX_BYTES = 67_108_864
_DEFAULT_EXECUTION: Literal['sync', 'async'] = 'sync'
DEFAULT_MAX_FAILURE_TEXT_BYTES = 262_144
DEFAULT_MAX_GROUP_OBLIGATIONS = 1_000
DEFAULT_MAX_IPC_FRAME_BYTES = 8_388_608
DEFAULT_MAX_LATE_IDS_PER_GROUP = 50_000
DEFAULT_MAX_MANIFEST_BYTES = 1_048_576
DEFAULT_MAX_OPEN_RANGES = 100
DEFAULT_MAX_PENDING_EPOCHS = 8
DEFAULT_MAX_PENDING_GROUP_OBLIGATIONS = 1_300
DEFAULT_MAX_RANGE_QUERY_PAGES = 100
DEFAULT_MAX_SOURCE_LATE_IDS = 50_000
_DEFAULT_ORDERING: Literal['eager', 'strict'] = 'eager'
DEFAULT_QUEUE_MAX_BYTES = 67_108_864
DEFAULT_QUEUE_MAX_EVENTS = 1_000
DEFAULT_SQLITE_SYNCHRONOUS: Literal['normal', 'full'] = 'normal'
DEFAULT_STRICT_HOLDBACK_SECS = 300.0
DEFAULT_STRICT_MAX_BYTES = 268_435_456
DEFAULT_STRICT_MAX_EVENTS = 20_000
DEFAULT_VISIBILITY_OVERLAP_SECS = 300.0
MIN_PENDING_EPOCHS = 2
_ACCEPTANCE_ALLOWED_KEYS = frozenset(
    {
        'captured_at',
        'observed_gap_rate',
        'site',
        'source',
        'source_fingerprint',
        'stream_incarnation',
    }
)
_ACCEPTANCE_REQUIRED_KEYS = frozenset(
    {
        'captured_at',
        'observed_gap_rate',
        'site',
        'source',
        'source_fingerprint',
    }
)
_DEFAULT_ASYNC_CANCEL_GRACE_SECS = 5.0
_DEFAULT_CALLBACK_DEADLINE_SECS = 120.0
_DEFAULT_FETCH_INTERVAL_SECS = 1.0
_DEFAULT_HEALTHY_RESET_SECS = 300.0
_DEFAULT_HEALTH_INTERVAL_SECS = 2.0
_DEFAULT_MAX_CONCURRENT_CATCHUP_READERS = 4
_DEFAULT_MAX_EVENT_BYTES = 4_194_304
_DEFAULT_MAX_PLUGIN_CONFIG_BYTES = 262_144
_DEFAULT_POLL_RETRY_BASE_SECS = 1.0
_DEFAULT_POLL_RETRY_JITTER_FRACTION = 0.2
_DEFAULT_POLL_RETRY_MAX_SECS = 60.0
_DEFAULT_POLL_RETRY_MULTIPLIER = 2.0
_DEFAULT_PROBE_BIND = str(ipaddress.IPv4Address(0))
_DEFAULT_RESTART_LIMIT = 5
_DEFAULT_RESTART_WINDOW_SECS = 300.0
_DEFAULT_RPC_ATTEMPT_INTERVAL_SECS = 3.0
_DEFAULT_RPC_MAX_ATTEMPTS = 3
_DEFAULT_RPC_TIMEOUT_SECS = 30.0
_DEFAULT_SHUTDOWN_DEADLINE_SECS = 10.0
_DEFAULT_SUPERVISOR_POLL_INTERVAL_SECS = 1.0
_DEFAULT_WORKER = 'default'
_DEFAULT_WORKER_STARTUP_DEADLINE_SECS = 30.0
_MAX_TCP_PORT = 65_535
_NAME_PATTERN = re.compile(r'[a-z][a-z0-9_-]{0,63}\Z')
_RESERVED_WORKER_NAMES = frozenset({'dedicated', 'default'})


class SecretValue:
    """Hold a resolved secret behind an explicit reveal operation."""

    __slots__ = ('__value',)

    def __init__(self, value: str) -> None:
        """Store one protected secret value."""
        object.__setattr__(self, '_SecretValue__value', value)

    def __deepcopy__(self, memo: dict[int, object]) -> SecretValue:
        """Reuse this immutable wrapper during deep copying."""
        memo[id(self)] = self
        return self

    def __repr__(self) -> str:
        """Return a redacted representation."""
        return '<redacted>'

    def __setattr__(self, name: str, value: object) -> None:
        """Reject mutation after construction."""
        raise AttributeError(f'{type(self).__name__}.{name} is immutable')

    def __str__(self) -> str:
        """Return a redacted string.

        Returns:
            The fixed redaction marker.
        """
        return '<redacted>'

    def reveal(self) -> str:
        """Return the protected secret value.

        Returns:
            The resolved secret.
        """
        return self.__value


class _GroupStateOptions(TypedDict):
    """Type the configuration policy shared by every group-state assembly."""

    synchronous: str
    max_failure_text_bytes: int
    max_group_obligations: int
    max_late_ids_per_group: int
    max_manifest_bytes: int
    max_pending_epochs: int
    strict_holdback_secs: float


class _SourceStateOptions(TypedDict):
    """Type the configuration policy shared by every source-state assembly."""

    synchronous: str
    visibility_overlap_secs: float
    max_open_ranges: int
    max_group_obligations: int
    max_range_query_pages: int
    max_pending_group_obligations: int
    max_source_late_ids: int
    max_ipc_frame_bytes: int
    commit_batch_max_bytes: int


@dataclass(frozen=True)
class AcceptanceSourceEvidence:
    """Record live acceptance observations for one source."""

    site: str
    source: str
    source_fingerprint: str
    stream_incarnation: str | None
    captured_at: datetime
    observed_gap_rate: float


@dataclass(frozen=True)
class AcceptanceEvidence:
    """Contain parsed live acceptance observations."""

    sources: tuple[AcceptanceSourceEvidence, ...]


class ConfigError(ValueError):
    """Report invalid daemon or acceptance configuration."""


@dataclass(frozen=True)
class ControlConfig:
    """Contain the state path needed by control-plane commands.

    Attributes:
        state_dir: Daemon state directory from which the supervisor control
            socket path is derived.
    """

    state_dir: Path


@dataclass(frozen=True)
class DaemonConfig:
    """Define deployment-wide daemon settings.

    Attributes:
        state_dir: Directory for durable daemon state.
        sg_api_provider: Selected SG API implementation.
        probe_bind: Address used for probe listeners.
        probe_port: Probe port, or none when probes are disabled.
        sqlite_synchronous: Durability mode for SQLite transactions.
        checkpoint_interval_secs: Interval between worker checkpoints.
        health_interval_secs: Interval between worker health updates.
        worker_startup_deadline_secs: Maximum worker startup time.
        supervisor_poll_interval_secs: Supervisor polling interval.
        shutdown_deadline_secs: Maximum graceful shutdown time.
        async_cancel_grace_secs: Grace period for async cancellation.
        max_ipc_frame_bytes: Largest accepted IPC frame.
        max_plugin_config_bytes: Largest plugin configuration.
        max_manifest_bytes: Largest callback manifest.
        max_failure_text_bytes: Largest persisted failure text.
        max_group_obligations: Bound on obligations per group.
        max_pending_epochs: Bound on pending strict epochs.
        max_concurrent_catchup_readers: Bound on concurrent catch-up reads.
    """

    state_dir: Path
    sg_api_provider: Literal['shotgun-api3', 'sg-python-api']
    probe_bind: str = _DEFAULT_PROBE_BIND
    probe_port: int | None = None
    sqlite_synchronous: Literal['normal', 'full'] = DEFAULT_SQLITE_SYNCHRONOUS
    checkpoint_interval_secs: float = DEFAULT_CHECKPOINT_INTERVAL_SECS
    health_interval_secs: float = _DEFAULT_HEALTH_INTERVAL_SECS
    worker_startup_deadline_secs: float = _DEFAULT_WORKER_STARTUP_DEADLINE_SECS
    supervisor_poll_interval_secs: float = _DEFAULT_SUPERVISOR_POLL_INTERVAL_SECS
    shutdown_deadline_secs: float = _DEFAULT_SHUTDOWN_DEADLINE_SECS
    async_cancel_grace_secs: float = _DEFAULT_ASYNC_CANCEL_GRACE_SECS
    max_ipc_frame_bytes: int = DEFAULT_MAX_IPC_FRAME_BYTES
    max_plugin_config_bytes: int = _DEFAULT_MAX_PLUGIN_CONFIG_BYTES
    max_manifest_bytes: int = DEFAULT_MAX_MANIFEST_BYTES
    max_failure_text_bytes: int = DEFAULT_MAX_FAILURE_TEXT_BYTES
    max_group_obligations: int = DEFAULT_MAX_GROUP_OBLIGATIONS
    max_pending_epochs: int = DEFAULT_MAX_PENDING_EPOCHS
    max_concurrent_catchup_readers: int = _DEFAULT_MAX_CONCURRENT_CATCHUP_READERS


@dataclass(frozen=True)
class SecretRef:
    """Identify a secret by environment variable name."""

    env: str


@dataclass(frozen=True)
class PluginConfig:
    """Define one explicitly ordered plugin."""

    id: str
    path: Path
    credentials: str
    config: Mapping[str, object]
    secrets: Mapping[str, SecretRef]


@dataclass(frozen=True)
class GroupConfig:
    """Define one serial callback group."""

    name: str
    source: str
    plugins: tuple[PluginConfig, ...]
    callback_credentials: str
    execution: Literal['sync', 'async'] = _DEFAULT_EXECUTION
    ordering: Literal['eager', 'strict'] = _DEFAULT_ORDERING
    strict_holdback_secs: float | None = None
    worker: str = _DEFAULT_WORKER
    callback_deadline_secs: float = _DEFAULT_CALLBACK_DEADLINE_SECS


@dataclass(frozen=True)
class CredentialConfig:
    """Define one SG script credential."""

    name: str
    script_name: str
    api_key: SecretValue
    timeout_secs: float = _DEFAULT_RPC_TIMEOUT_SECS
    max_rpc_attempts: int = _DEFAULT_RPC_MAX_ATTEMPTS
    rpc_attempt_interval_secs: float = _DEFAULT_RPC_ATTEMPT_INTERVAL_SECS


@dataclass(frozen=True)
class SourceConfig:
    """Define one independently polled SG event source."""

    name: str
    credentials: str
    fetch_interval_secs: float = _DEFAULT_FETCH_INTERVAL_SECS
    batch_size: int = DEFAULT_BATCH_SIZE
    visibility_overlap_secs: float = DEFAULT_VISIBILITY_OVERLAP_SECS
    poll_retry_base_secs: float = _DEFAULT_POLL_RETRY_BASE_SECS
    poll_retry_max_secs: float = _DEFAULT_POLL_RETRY_MAX_SECS
    poll_retry_multiplier: float = _DEFAULT_POLL_RETRY_MULTIPLIER
    poll_retry_jitter_fraction: float = _DEFAULT_POLL_RETRY_JITTER_FRACTION
    max_event_bytes: int = _DEFAULT_MAX_EVENT_BYTES
    commit_batch_max_bytes: int = DEFAULT_COMMIT_BATCH_MAX_BYTES
    queue_max_events: int = DEFAULT_QUEUE_MAX_EVENTS
    queue_max_bytes: int = DEFAULT_QUEUE_MAX_BYTES
    strict_max_events: int = DEFAULT_STRICT_MAX_EVENTS
    strict_max_bytes: int = DEFAULT_STRICT_MAX_BYTES
    max_open_ranges: int = DEFAULT_MAX_OPEN_RANGES
    max_range_query_pages: int = DEFAULT_MAX_RANGE_QUERY_PAGES
    max_pending_group_obligations: int = DEFAULT_MAX_PENDING_GROUP_OBLIGATIONS
    max_source_late_ids: int = DEFAULT_MAX_SOURCE_LATE_IDS
    max_late_ids_per_group: int = DEFAULT_MAX_LATE_IDS_PER_GROUP


@dataclass(frozen=True)
class WorkerConfig:
    """Define one named worker pool."""

    name: str
    restart_limit: int = _DEFAULT_RESTART_LIMIT
    restart_window_secs: float = _DEFAULT_RESTART_WINDOW_SECS
    healthy_reset_secs: float = _DEFAULT_HEALTHY_RESET_SECS


@dataclass(frozen=True)
class SiteConfig:
    """Define one SG site and its resolved child configuration."""

    name: str
    server: str
    default_source: str
    credentials: Mapping[str, CredentialConfig]
    sources: Mapping[str, SourceConfig]
    groups: Mapping[str, GroupConfig]
    workers: Mapping[str, WorkerConfig]


@dataclass(frozen=True)
class Config:
    """Contain one fully resolved daemon configuration."""

    daemon: DaemonConfig
    sites: Mapping[str, SiteConfig]


@dataclass(frozen=True)
class _GroupLoadContext:
    """Carry shared group-loader inputs."""

    _config_path: Path
    _credentials: Mapping[str, CredentialConfig]
    _daemon: DaemonConfig
    _default_source: str
    _site_name: str
    _sources: Mapping[str, SourceConfig]
    _workers: Mapping[str, WorkerConfig]


@dataclass(frozen=True)
class _PluginLoadContext:
    """Carry shared plugin-loader inputs."""

    _config_path: Path
    _credentials: Mapping[str, CredentialConfig]
    _default_credentials: str
    _group_name: str
    _max_config_bytes: int
    _site_name: str


def _canonical_plugin_bytes(value: Mapping[str, object]) -> bytes:
    """Return canonical compact JSON for validated plugin configuration."""
    return canonical_bytes(value)


def _check_keys(
    table: Mapping[str, object],
    *,
    allowed: frozenset[str],
    context: str,
    required: frozenset[str] = frozenset(),
) -> None:
    """Reject unknown or missing table keys."""
    unknown = sorted(table.keys() - allowed)
    if unknown:
        raise ConfigError(f'{context} has unknown keys: {", ".join(unknown)}')
    missing = sorted(required - table.keys())
    if missing:
        raise ConfigError(f'{context} has missing keys: {", ".join(missing)}')


def _finite_float(
    table: Mapping[str, object],
    key: str,
    context: str,
    default: float,
) -> float:
    """Read one finite floating-point setting."""
    value = table.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ConfigError(f'{context}.{key} must be a finite number')
    result = float(value)
    if not math.isfinite(result):
        raise ConfigError(f'{context}.{key} must be a finite number')
    return result


def _freeze_json(value: object, context: str) -> object:
    """Validate and recursively freeze one JSON-compatible value."""
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ConfigError(f'{context} must contain only finite JSON-compatible values')
        return value
    if isinstance(value, list):
        items = cast('list[object]', value)
        return tuple(_freeze_json(item, context) for item in items)
    if isinstance(value, Mapping):
        items = cast('Mapping[object, object]', value)
        if not all(isinstance(key, str) for key in items):
            raise ConfigError(f'{context} must contain only string object keys')
        frozen = {cast('str', key): _freeze_json(item, context) for key, item in items.items()}
        return MappingProxyType(frozen)
    raise ConfigError(f'{context} must contain only JSON-compatible values')


def _mapping(value: object, context: str) -> Mapping[str, object]:
    """Require a TOML table."""
    if not isinstance(value, dict):
        raise ConfigError(f'{context} must be a table')
    return cast('Mapping[str, object]', value)


def _optional_mapping(
    table: Mapping[str, object],
    key: str,
    context: str,
) -> Mapping[str, object]:
    """Read an optional TOML table."""
    if key not in table:
        return MappingProxyType({})
    return _mapping(table[key], f'{context}.{key}')


def _optional_probe_port(
    table: Mapping[str, object],
    key: str,
    context: str,
) -> int | None:
    """Read one optional TCP port."""
    value = table.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= _MAX_TCP_PORT:
        raise ConfigError(f'{context}.{key} must be an integer from 1 through 65535')
    return value


def _optional_string(
    table: Mapping[str, object],
    key: str,
    context: str,
    default: str,
) -> str:
    """Read an optional string."""
    value = table.get(key, default)
    if not isinstance(value, str) or not value:
        raise ConfigError(f'{context}.{key} must be a nonempty string')
    return value


def _parse_toml(path: Path, label: str) -> Mapping[str, object]:
    """Read and parse one TOML file, separating unreadable files from invalid TOML."""
    try:
        content = path.read_bytes()
    except OSError as error:
        reason = error.strerror or str(error)
        raise ConfigError(f'cannot read {label} {path}: {reason}') from error
    try:
        return cast('Mapping[str, object]', tomllib.loads(content.decode('utf-8')))
    except UnicodeDecodeError as error:
        raise ConfigError(f'invalid TOML in {path}: the file is not UTF-8 text') from error
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f'invalid TOML in {path}: {error}') from error


def _positive_float(
    table: Mapping[str, object],
    key: str,
    context: str,
    default: float,
) -> float:
    """Read one positive finite floating-point setting."""
    raw_value = table.get(key, default)
    if isinstance(raw_value, bool) or not isinstance(raw_value, int | float):
        raise ConfigError(f'{context}.{key} must be positive and finite')
    value = float(raw_value)
    if not math.isfinite(value) or value <= 0:
        raise ConfigError(f'{context}.{key} must be positive and finite')
    return value


def _positive_int(
    table: Mapping[str, object],
    key: str,
    context: str,
    default: int,
) -> int:
    """Read one positive integer setting."""
    value = table.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ConfigError(f'{context}.{key} must be an integer at least 1')
    return value


def _required_mapping(
    table: Mapping[str, object],
    key: str,
    context: str,
) -> Mapping[str, object]:
    """Read a required TOML table."""
    if key not in table:
        raise ConfigError(f'{context} has missing keys: {key}')
    return _mapping(table[key], f'{context}.{key}')


def _required_string(
    table: Mapping[str, object],
    key: str,
    context: str,
) -> str:
    """Read a required nonempty string."""
    if key not in table:
        raise ConfigError(f'{context} has missing keys: {key}')
    value = table[key]
    if not isinstance(value, str) or not value:
        raise ConfigError(f'{context}.{key} must be a nonempty string')
    return value


def _secret_ref(value: object, context: str) -> SecretRef:
    """Validate one exact environment reference and its availability."""
    if not isinstance(value, dict):
        raise ConfigError(f'{context} must be an exact environment reference')
    table = cast('Mapping[str, object]', value)
    _check_keys(
        table,
        allowed=frozenset({'env'}),
        context=context,
        required=frozenset({'env'}),
    )
    env_name = _required_string(table, 'env', context)
    if env_name not in os.environ:
        raise ConfigError(f'{context} environment variable {env_name} is not set')
    return SecretRef(env=env_name)


def _state_directory(table: Mapping[str, object], config_path: Path) -> Path:
    """Resolve the state directory relative to the configuration file."""
    path = Path(_required_string(table, 'state_dir', 'daemon')).expanduser()
    return path if path.is_absolute() else config_path.resolve().parent / path


def _load_daemon(table: Mapping[str, object], config_path: Path) -> DaemonConfig:
    """Load deployment-wide daemon configuration."""
    context = 'daemon'
    _check_keys(
        table,
        allowed=frozenset(
            {
                'async_cancel_grace_secs',
                'checkpoint_interval_secs',
                'health_interval_secs',
                'max_concurrent_catchup_readers',
                'max_failure_text_bytes',
                'max_group_obligations',
                'max_ipc_frame_bytes',
                'max_manifest_bytes',
                'max_pending_epochs',
                'max_plugin_config_bytes',
                'probe_bind',
                'probe_port',
                'sg_api_provider',
                'shutdown_deadline_secs',
                'sqlite_synchronous',
                'state_dir',
                'supervisor_poll_interval_secs',
                'worker_startup_deadline_secs',
            }
        ),
        context=context,
        required=frozenset({'sg_api_provider', 'state_dir'}),
    )
    provider_value = _required_string(table, 'sg_api_provider', context)
    if provider_value not in SUPPORTED_PROVIDERS:
        raise ConfigError('daemon.sg_api_provider must be shotgun-api3 or sg-python-api')
    provider = cast("Literal['shotgun-api3', 'sg-python-api']", provider_value)
    probe_bind = _optional_string(table, 'probe_bind', context, _DEFAULT_PROBE_BIND)
    try:
        ipaddress.ip_address(probe_bind)
    except ValueError as error:
        raise ConfigError('daemon.probe_bind must be an IP address literal') from error
    synchronous_value = _optional_string(
        table,
        'sqlite_synchronous',
        context,
        DEFAULT_SQLITE_SYNCHRONOUS,
    )
    if synchronous_value not in {'normal', 'full'}:
        raise ConfigError('daemon.sqlite_synchronous must be normal or full')
    synchronous = cast("Literal['normal', 'full']", synchronous_value)
    max_pending_epochs = _positive_int(
        table, 'max_pending_epochs', context, DEFAULT_MAX_PENDING_EPOCHS
    )
    if max_pending_epochs < MIN_PENDING_EPOCHS:
        raise ConfigError('daemon.max_pending_epochs must be at least 2')
    return DaemonConfig(
        state_dir=_state_directory(table, config_path),
        sg_api_provider=provider,
        probe_bind=probe_bind,
        probe_port=_optional_probe_port(table, 'probe_port', context),
        sqlite_synchronous=synchronous,
        checkpoint_interval_secs=_positive_float(
            table,
            'checkpoint_interval_secs',
            context,
            DEFAULT_CHECKPOINT_INTERVAL_SECS,
        ),
        health_interval_secs=_positive_float(
            table, 'health_interval_secs', context, _DEFAULT_HEALTH_INTERVAL_SECS
        ),
        worker_startup_deadline_secs=_positive_float(
            table,
            'worker_startup_deadline_secs',
            context,
            _DEFAULT_WORKER_STARTUP_DEADLINE_SECS,
        ),
        supervisor_poll_interval_secs=_positive_float(
            table,
            'supervisor_poll_interval_secs',
            context,
            _DEFAULT_SUPERVISOR_POLL_INTERVAL_SECS,
        ),
        shutdown_deadline_secs=_positive_float(
            table,
            'shutdown_deadline_secs',
            context,
            _DEFAULT_SHUTDOWN_DEADLINE_SECS,
        ),
        async_cancel_grace_secs=_positive_float(
            table,
            'async_cancel_grace_secs',
            context,
            _DEFAULT_ASYNC_CANCEL_GRACE_SECS,
        ),
        max_ipc_frame_bytes=_positive_int(
            table, 'max_ipc_frame_bytes', context, DEFAULT_MAX_IPC_FRAME_BYTES
        ),
        max_plugin_config_bytes=_positive_int(
            table,
            'max_plugin_config_bytes',
            context,
            _DEFAULT_MAX_PLUGIN_CONFIG_BYTES,
        ),
        max_manifest_bytes=_positive_int(
            table, 'max_manifest_bytes', context, DEFAULT_MAX_MANIFEST_BYTES
        ),
        max_failure_text_bytes=_positive_int(
            table,
            'max_failure_text_bytes',
            context,
            DEFAULT_MAX_FAILURE_TEXT_BYTES,
        ),
        max_group_obligations=_positive_int(
            table,
            'max_group_obligations',
            context,
            DEFAULT_MAX_GROUP_OBLIGATIONS,
        ),
        max_pending_epochs=max_pending_epochs,
        max_concurrent_catchup_readers=_positive_int(
            table,
            'max_concurrent_catchup_readers',
            context,
            _DEFAULT_MAX_CONCURRENT_CATCHUP_READERS,
        ),
    )


def _strict_group_holdback_secs(group: GroupConfig, source: SourceConfig) -> float:
    """Return the positive stored holdback for eager or strict groups."""
    return (
        source.visibility_overlap_secs
        if group.strict_holdback_secs is None
        else group.strict_holdback_secs
    )


def _validate_name(value: str, context: str) -> str:
    """Validate one bounded configuration identifier."""
    if _NAME_PATTERN.fullmatch(value) is None:
        raise ConfigError(f'invalid {context} name {value!r}')
    return value


def _load_acceptance_source(
    table: Mapping[str, object],
    index: int,
) -> AcceptanceSourceEvidence:
    """Load one acceptance source record."""
    context = f'sources[{index}]'
    _check_keys(
        table,
        allowed=_ACCEPTANCE_ALLOWED_KEYS,
        context=context,
        required=_ACCEPTANCE_REQUIRED_KEYS,
    )
    site = _validate_name(_required_string(table, 'site', context), 'site')
    source = _validate_name(_required_string(table, 'source', context), 'source')
    source_fingerprint = _required_string(table, 'source_fingerprint', context)
    incarnation_value = table.get('stream_incarnation')
    if incarnation_value is not None and (
        not isinstance(incarnation_value, str) or not incarnation_value
    ):
        raise ConfigError(f'{context}.stream_incarnation must be a nonempty string')
    captured_at = table['captured_at']
    if not isinstance(captured_at, datetime) or captured_at.utcoffset() is None:
        raise ConfigError(f'{context}.captured_at must be an offset date-time')
    observed_gap_rate = table['observed_gap_rate']
    if (
        isinstance(observed_gap_rate, bool)
        or not isinstance(observed_gap_rate, float)
        or not math.isfinite(observed_gap_rate)
        or observed_gap_rate < 0
    ):
        raise ConfigError(f'{context}.observed_gap_rate must be a nonnegative finite float')
    return AcceptanceSourceEvidence(
        site=site,
        source=source,
        source_fingerprint=source_fingerprint,
        stream_incarnation=incarnation_value,
        captured_at=captured_at,
        observed_gap_rate=observed_gap_rate,
    )


def _load_credential(
    name: str,
    table: Mapping[str, object],
    site_name: str,
) -> CredentialConfig:
    """Load one credential configuration."""
    context = f'sites.{site_name}.credentials.{name}'
    _check_keys(
        table,
        allowed=frozenset(
            {
                'api_key',
                'max_rpc_attempts',
                'rpc_attempt_interval_secs',
                'script_name',
                'timeout_secs',
            }
        ),
        context=context,
        required=frozenset({'api_key', 'script_name'}),
    )
    secret_ref = _secret_ref(table['api_key'], f'{context}.api_key')
    return CredentialConfig(
        name=_validate_name(name, 'credential'),
        script_name=_required_string(table, 'script_name', context),
        api_key=SecretValue(os.environ[secret_ref.env]),
        timeout_secs=_positive_float(table, 'timeout_secs', context, _DEFAULT_RPC_TIMEOUT_SECS),
        max_rpc_attempts=_positive_int(
            table, 'max_rpc_attempts', context, _DEFAULT_RPC_MAX_ATTEMPTS
        ),
        rpc_attempt_interval_secs=_positive_float(
            table,
            'rpc_attempt_interval_secs',
            context,
            _DEFAULT_RPC_ATTEMPT_INTERVAL_SECS,
        ),
    )


def _load_credentials(
    table: Mapping[str, object],
    site_name: str,
) -> Mapping[str, CredentialConfig]:
    """Load all credentials for one site."""
    if not table:
        raise ConfigError(f'sites.{site_name}.credentials must not be empty')
    credentials = {
        name: _load_credential(name, _mapping(value, f'credential {name}'), site_name)
        for name, value in table.items()
    }
    return MappingProxyType(credentials)


def _load_plugin(
    table: Mapping[str, object],
    load_context: _PluginLoadContext,
) -> PluginConfig:
    """Load one plugin and resolve its effective credentials."""
    context = f'sites.{load_context._site_name}.groups.{load_context._group_name}.plugins'
    _check_keys(
        table,
        allowed=frozenset({'config', 'credentials', 'id', 'path', 'secrets'}),
        context=context,
        required=frozenset({'id', 'path'}),
    )
    plugin_id = _validate_name(_required_string(table, 'id', context), 'plugin')
    path_value = Path(_required_string(table, 'path', context)).expanduser()
    if not path_value.is_absolute():
        path_value = load_context._config_path.parent / path_value
    path_value = path_value.resolve()
    if not path_value.is_file():
        raise ConfigError(
            f'{context}.{plugin_id} plugin path must be a regular file: {path_value}'
        )
    credential_name = _optional_string(
        table,
        'credentials',
        context,
        load_context._default_credentials,
    )
    if credential_name not in load_context._credentials:
        raise ConfigError(
            f'{context}.{plugin_id} references unknown credentials {credential_name}'
        )
    config_table = _optional_mapping(table, 'config', context)
    frozen_config = cast(
        'Mapping[str, object]',
        _freeze_json(config_table, f'{context}.{plugin_id} plugin config'),
    )
    if len(_canonical_plugin_bytes(config_table)) > load_context._max_config_bytes:
        raise ConfigError(
            f'{context}.{plugin_id} plugin config exceeds {load_context._max_config_bytes} bytes'
        )
    secrets_table = _optional_mapping(table, 'secrets', context)
    secret_refs = {
        name: _secret_ref(value, f'{context}.{plugin_id}.secrets.{name}')
        for name, value in secrets_table.items()
    }
    return PluginConfig(
        id=plugin_id,
        path=path_value,
        credentials=credential_name,
        config=frozen_config,
        secrets=MappingProxyType(secret_refs),
    )


def _load_plugins(
    value: object,
    load_context: _PluginLoadContext,
) -> tuple[PluginConfig, ...]:
    """Load the explicitly ordered plugins for one group."""
    if not isinstance(value, list) or not value:
        raise ConfigError(
            f'sites.{load_context._site_name}.groups.'
            f'{load_context._group_name}.plugins must be a nonempty array'
        )
    raw_plugins = cast('list[object]', value)
    plugins = tuple(
        _load_plugin(
            _mapping(
                item,
                f'sites.{load_context._site_name}.groups.'
                f'{load_context._group_name}.plugins[{index}]',
            ),
            load_context,
        )
        for index, item in enumerate(raw_plugins)
    )
    ids = [plugin.id for plugin in plugins]
    duplicates = sorted({plugin_id for plugin_id in ids if ids.count(plugin_id) > 1})
    if duplicates:
        raise ConfigError(
            f'sites.{load_context._site_name}.groups.'
            f'{load_context._group_name} has duplicate plugin id: '
            f'{", ".join(duplicates)}'
        )
    return plugins


def _load_group(
    name: str,
    table: Mapping[str, object],
    load_context: _GroupLoadContext,
) -> GroupConfig:
    """Load one group and resolve all local references."""
    context = f'sites.{load_context._site_name}.groups.{name}'
    _check_keys(
        table,
        allowed=frozenset(
            {
                'callback_credentials',
                'callback_deadline_secs',
                'execution',
                'ordering',
                'plugins',
                'source',
                'strict_holdback_secs',
                'worker',
            }
        ),
        context=context,
        required=frozenset({'plugins'}),
    )
    group_name = _validate_name(name, 'group')
    source_name = _optional_string(
        table,
        'source',
        context,
        load_context._default_source,
    )
    if source_name not in load_context._sources:
        raise ConfigError(
            f'site {load_context._site_name} group {name} references unknown source {source_name}'
        )
    source = load_context._sources[source_name]
    callback_credentials = _optional_string(
        table,
        'callback_credentials',
        context,
        source.credentials,
    )
    if callback_credentials not in load_context._credentials:
        raise ConfigError(f'{context} references unknown credentials {callback_credentials}')
    execution_value = _optional_string(table, 'execution', context, _DEFAULT_EXECUTION)
    if execution_value not in {'async', 'sync'}:
        raise ConfigError(f'{context}.execution must be sync or async')
    execution = cast("Literal['sync', 'async']", execution_value)
    if execution == 'async' and load_context._daemon.sg_api_provider == 'shotgun-api3':
        raise ConfigError(f'{context} async execution requires sg-python-api')
    ordering_value = _optional_string(table, 'ordering', context, _DEFAULT_ORDERING)
    if ordering_value not in {'eager', 'strict'}:
        raise ConfigError(f'{context}.ordering must be eager or strict')
    ordering = cast("Literal['eager', 'strict']", ordering_value)
    if ordering == 'eager' and 'strict_holdback_secs' in table:
        raise ConfigError(f'{context}.strict_holdback_secs is valid only for strict ordering')
    strict_holdback = (
        _positive_float(table, 'strict_holdback_secs', context, DEFAULT_STRICT_HOLDBACK_SECS)
        if ordering == 'strict'
        else None
    )
    worker = _optional_string(table, 'worker', context, _DEFAULT_WORKER)
    if worker not in {'default', 'dedicated'} and worker not in load_context._workers:
        raise ConfigError(f'{context} references unknown worker {worker}')
    callback_deadline = _positive_float(
        table,
        'callback_deadline_secs',
        context,
        _DEFAULT_CALLBACK_DEADLINE_SECS,
    )
    credential = load_context._credentials[callback_credentials]
    provider_budget = provider_budget_secs(credential)
    if callback_deadline < provider_budget:
        warnings.warn(
            f'{context} callback deadline {callback_deadline} is below '
            f'the nominal provider budget {provider_budget}',
            UserWarning,
            stacklevel=2,
        )
    plugins = _load_plugins(
        table['plugins'],
        _PluginLoadContext(
            _config_path=load_context._config_path,
            _credentials=load_context._credentials,
            _default_credentials=callback_credentials,
            _group_name=group_name,
            _max_config_bytes=load_context._daemon.max_plugin_config_bytes,
            _site_name=load_context._site_name,
        ),
    )
    return GroupConfig(
        name=group_name,
        source=source_name,
        plugins=plugins,
        callback_credentials=callback_credentials,
        execution=execution,
        ordering=ordering,
        strict_holdback_secs=strict_holdback,
        worker=worker,
        callback_deadline_secs=callback_deadline,
    )


def _load_groups(
    table: Mapping[str, object],
    load_context: _GroupLoadContext,
) -> Mapping[str, GroupConfig]:
    """Load all callback groups for one site."""
    if not table:
        raise ConfigError(f'sites.{load_context._site_name}.groups must not be empty')
    groups = {
        name: _load_group(
            name,
            _mapping(value, f'sites.{load_context._site_name}.groups.{name}'),
            load_context,
        )
        for name, value in table.items()
    }
    return MappingProxyType(groups)


def _load_source(
    name: str,
    table: Mapping[str, object],
    credentials: Mapping[str, CredentialConfig],
    site_name: str,
) -> SourceConfig:
    """Load one source configuration."""
    context = f'sites.{site_name}.sources.{name}'
    _check_keys(
        table,
        allowed=frozenset(
            {
                'batch_size',
                'commit_batch_max_bytes',
                'credentials',
                'fetch_interval_secs',
                'max_event_bytes',
                'max_late_ids_per_group',
                'max_open_ranges',
                'max_pending_group_obligations',
                'max_range_query_pages',
                'max_source_late_ids',
                'poll_retry_base_secs',
                'poll_retry_jitter_fraction',
                'poll_retry_max_secs',
                'poll_retry_multiplier',
                'queue_max_bytes',
                'queue_max_events',
                'strict_max_bytes',
                'strict_max_events',
                'visibility_overlap_secs',
            }
        ),
        context=context,
        required=frozenset({'credentials'}),
    )
    credential_name = _required_string(table, 'credentials', context)
    if credential_name not in credentials:
        raise ConfigError(f'{context} references unknown credentials {credential_name}')
    retry_base = _positive_float(
        table, 'poll_retry_base_secs', context, _DEFAULT_POLL_RETRY_BASE_SECS
    )
    retry_max = _positive_float(
        table, 'poll_retry_max_secs', context, _DEFAULT_POLL_RETRY_MAX_SECS
    )
    if retry_max < retry_base:
        raise ConfigError(f'{context}.poll_retry_max_secs must be at least poll_retry_base_secs')
    retry_multiplier = _finite_float(
        table, 'poll_retry_multiplier', context, _DEFAULT_POLL_RETRY_MULTIPLIER
    )
    if retry_multiplier < 1:
        raise ConfigError(f'{context}.poll_retry_multiplier must be at least 1')
    jitter = _finite_float(
        table, 'poll_retry_jitter_fraction', context, _DEFAULT_POLL_RETRY_JITTER_FRACTION
    )
    if not 0 <= jitter < 1:
        raise ConfigError(f'{context}.poll_retry_jitter_fraction must be in [0, 1)')
    return SourceConfig(
        name=_validate_name(name, 'source'),
        credentials=credential_name,
        fetch_interval_secs=_positive_float(
            table, 'fetch_interval_secs', context, _DEFAULT_FETCH_INTERVAL_SECS
        ),
        batch_size=_positive_int(table, 'batch_size', context, DEFAULT_BATCH_SIZE),
        visibility_overlap_secs=_positive_float(
            table,
            'visibility_overlap_secs',
            context,
            DEFAULT_VISIBILITY_OVERLAP_SECS,
        ),
        poll_retry_base_secs=retry_base,
        poll_retry_max_secs=retry_max,
        poll_retry_multiplier=retry_multiplier,
        poll_retry_jitter_fraction=jitter,
        max_event_bytes=_positive_int(table, 'max_event_bytes', context, _DEFAULT_MAX_EVENT_BYTES),
        commit_batch_max_bytes=_positive_int(
            table,
            'commit_batch_max_bytes',
            context,
            DEFAULT_COMMIT_BATCH_MAX_BYTES,
        ),
        queue_max_events=_positive_int(
            table, 'queue_max_events', context, DEFAULT_QUEUE_MAX_EVENTS
        ),
        queue_max_bytes=_positive_int(table, 'queue_max_bytes', context, DEFAULT_QUEUE_MAX_BYTES),
        strict_max_events=_positive_int(
            table, 'strict_max_events', context, DEFAULT_STRICT_MAX_EVENTS
        ),
        strict_max_bytes=_positive_int(
            table, 'strict_max_bytes', context, DEFAULT_STRICT_MAX_BYTES
        ),
        max_open_ranges=_positive_int(table, 'max_open_ranges', context, DEFAULT_MAX_OPEN_RANGES),
        max_range_query_pages=_positive_int(
            table, 'max_range_query_pages', context, DEFAULT_MAX_RANGE_QUERY_PAGES
        ),
        max_pending_group_obligations=_positive_int(
            table,
            'max_pending_group_obligations',
            context,
            DEFAULT_MAX_PENDING_GROUP_OBLIGATIONS,
        ),
        max_source_late_ids=_positive_int(
            table,
            'max_source_late_ids',
            context,
            DEFAULT_MAX_SOURCE_LATE_IDS,
        ),
        max_late_ids_per_group=_positive_int(
            table,
            'max_late_ids_per_group',
            context,
            DEFAULT_MAX_LATE_IDS_PER_GROUP,
        ),
    )


def _load_sources(
    table: Mapping[str, object],
    credentials: Mapping[str, CredentialConfig],
    site_name: str,
) -> Mapping[str, SourceConfig]:
    """Load all sources for one site and reject polling aliases."""
    if not table:
        raise ConfigError(f'sites.{site_name}.sources must not be empty')
    sources = {
        name: _load_source(
            name,
            _mapping(value, f'sites.{site_name}.sources.{name}'),
            credentials,
            site_name,
        )
        for name, value in table.items()
    }
    identities: dict[str, str] = {}
    socket_names: dict[str, str] = {}
    for source in sources.values():
        socket_name = source_socket_kind(source.name)
        if socket_name in socket_names:
            raise ConfigError(
                f'site {site_name} sources {socket_names[socket_name]} and {source.name} '
                'share an event socket name'
            )
        socket_names[socket_name] = source.name
        script_name = credentials[source.credentials].script_name
        if script_name in identities:
            raise ConfigError(
                f'site {site_name} has duplicate polling identity {script_name!r} '
                f'for sources {identities[script_name]} and {source.name}'
            )
        identities[script_name] = source.name
    return MappingProxyType(sources)


def _load_worker(
    name: str,
    table: Mapping[str, object],
    site_name: str,
) -> WorkerConfig:
    """Load one named worker configuration."""
    context = f'sites.{site_name}.workers.{name}'
    _validate_name(name, 'worker')
    if name in _RESERVED_WORKER_NAMES:
        raise ConfigError(f'reserved worker name {name}')
    _check_keys(
        table,
        allowed=frozenset({'healthy_reset_secs', 'restart_limit', 'restart_window_secs'}),
        context=context,
    )
    return WorkerConfig(
        name=name,
        restart_limit=_positive_int(table, 'restart_limit', context, _DEFAULT_RESTART_LIMIT),
        restart_window_secs=_positive_float(
            table, 'restart_window_secs', context, _DEFAULT_RESTART_WINDOW_SECS
        ),
        healthy_reset_secs=_positive_float(
            table, 'healthy_reset_secs', context, _DEFAULT_HEALTHY_RESET_SECS
        ),
    )


def _load_workers(
    table: Mapping[str, object],
    site_name: str,
) -> Mapping[str, WorkerConfig]:
    """Load all named workers for one site."""
    workers = {
        name: _load_worker(
            name,
            _mapping(value, f'sites.{site_name}.workers.{name}'),
            site_name,
        )
        for name, value in table.items()
    }
    return MappingProxyType(workers)


def _validate_source_limits(
    daemon: DaemonConfig,
    site_name: str,
    sources: Mapping[str, SourceConfig],
    groups: Mapping[str, GroupConfig],
) -> None:
    """Validate relationships between daemon, source, and group limits."""
    maximum_integer = 2**63 - 1
    for source in sources.values():
        context = f'sites.{site_name}.sources.{source.name}'
        framed_event_bytes = event_frame_bytes(
            'event',
            {'event_id': maximum_integer},
            payload_byte_count=source.max_event_bytes,
        )
        if framed_event_bytes - 4 > daemon.max_ipc_frame_bytes:
            raise ConfigError(f'{context}.max_event_bytes does not fit max_ipc_frame_bytes')
        if source.queue_max_bytes < daemon.max_ipc_frame_bytes:
            raise ConfigError(f'{context}.queue_max_bytes must hold max_ipc_frame_bytes')
        if source.commit_batch_max_bytes < framed_event_bytes:
            raise ConfigError(
                f'{context}.commit_batch_max_bytes must hold one maximum framed event'
            )
        source_groups = tuple(group for group in groups.values() if group.source == source.name)
        strict = any(group.ordering == 'strict' for group in source_groups)
        if strict and source.strict_max_bytes < framed_event_bytes:
            raise ConfigError(f'{context}.strict_max_bytes must hold one maximum framed event')
        if source.max_pending_group_obligations < len(source_groups):
            raise ConfigError(
                f'{context}.max_pending_group_obligations must cover configured group count'
            )
        if daemon.max_group_obligations < source.max_open_ranges:
            raise ConfigError(
                f'daemon.max_group_obligations must be at least {context}.max_open_ranges'
            )


def _load_site(
    name: str,
    table: Mapping[str, object],
    daemon: DaemonConfig,
    config_path: Path,
) -> SiteConfig:
    """Load one site and validate all local references."""
    context = f'sites.{name}'
    site_name = _validate_name(name, 'site')
    _check_keys(
        table,
        allowed=frozenset(
            {'credentials', 'default_source', 'groups', 'server', 'sources', 'workers'}
        ),
        context=context,
        required=frozenset({'credentials', 'default_source', 'groups', 'server', 'sources'}),
    )
    server = _required_string(table, 'server', context)
    credentials = _load_credentials(_required_mapping(table, 'credentials', context), site_name)
    sources = _load_sources(
        _required_mapping(table, 'sources', context),
        credentials,
        site_name,
    )
    default_source = _required_string(table, 'default_source', context)
    if default_source not in sources:
        raise ConfigError(f'{context}.default_source references unknown source {default_source}')
    workers = _load_workers(_optional_mapping(table, 'workers', context), site_name)
    groups = _load_groups(
        _required_mapping(table, 'groups', context),
        _GroupLoadContext(
            _config_path=config_path,
            _credentials=credentials,
            _daemon=daemon,
            _default_source=default_source,
            _site_name=site_name,
            _sources=sources,
            _workers=workers,
        ),
    )
    _validate_source_limits(daemon, site_name, sources, groups)
    return SiteConfig(
        name=site_name,
        server=server,
        default_source=default_source,
        credentials=credentials,
        sources=sources,
        groups=groups,
        workers=workers,
    )


def group_state_options(
    daemon: DaemonConfig, source: SourceConfig, group: GroupConfig | None
) -> _GroupStateOptions:
    """Build group authority policy from configuration.

    Args:
        daemon: Resolved daemon policy.
        source: The group's owning source configuration.
        group: Configured group, or None for an absent orphan using source
            holdback.

    Returns:
        Options shared by normal and disposition state opens.
    """
    return {
        'synchronous': daemon.sqlite_synchronous.upper(),
        'max_failure_text_bytes': daemon.max_failure_text_bytes,
        'max_group_obligations': daemon.max_group_obligations,
        'max_late_ids_per_group': source.max_late_ids_per_group,
        'max_manifest_bytes': daemon.max_manifest_bytes,
        'max_pending_epochs': daemon.max_pending_epochs,
        'strict_holdback_secs': (
            source.visibility_overlap_secs
            if group is None
            else _strict_group_holdback_secs(group, source)
        ),
    }


def load_acceptance_evidence(path: Path) -> AcceptanceEvidence:
    """Load strict acceptance evidence.

    Args:
        path: TOML acceptance record path.

    Returns:
        Parsed immutable evidence.

    Raises:
        ConfigError: The file cannot be read, or the file or any evidence
            record is invalid.
    """
    root = _parse_toml(path, 'acceptance evidence file')
    _check_keys(
        root,
        allowed=frozenset({'sources'}),
        context='acceptance evidence',
        required=frozenset({'sources'}),
    )
    raw_sources = root['sources']
    if not isinstance(raw_sources, list):
        raise ConfigError('acceptance evidence.sources must be an array of tables')
    raw_source_list = cast('list[object]', raw_sources)
    sources = tuple(
        _load_acceptance_source(_mapping(value, f'sources[{index}]'), index)
        for index, value in enumerate(raw_source_list)
    )
    seen: set[tuple[str, str]] = set()
    for source in sources:
        pair = (source.site, source.source)
        if pair in seen:
            raise ConfigError(f'duplicate acceptance evidence for {source.site}/{source.source}')
        seen.add(pair)
    return AcceptanceEvidence(sources=sources)


def load_config(path: Path) -> Config:
    """Load and fully resolve daemon configuration.

    Emits a `UserWarning` when a callback deadline is below its provider
    budget.

    Args:
        path: TOML daemon configuration path.

    Returns:
        Fully resolved immutable configuration.

    Raises:
        ConfigError: The file cannot be read, is malformed, or violates the
            configuration contract.
    """
    root = _parse_toml(path, 'configuration file')
    _check_keys(
        root,
        allowed=frozenset({'daemon', 'sites'}),
        context='configuration',
        required=frozenset({'daemon', 'sites'}),
    )
    daemon = _load_daemon(_required_mapping(root, 'daemon', 'configuration'), path)
    sites_table = _required_mapping(root, 'sites', 'configuration')
    if not sites_table:
        raise ConfigError('configuration.sites must not be empty')
    sites = {
        name: _load_site(
            name,
            _mapping(value, f'sites.{name}'),
            daemon,
            path.resolve(),
        )
        for name, value in sites_table.items()
    }
    return Config(daemon=daemon, sites=MappingProxyType(sites))


def load_control_config(path: Path) -> ControlConfig:
    """Load only the state path required by control commands.

    Args:
        path: TOML daemon configuration path.

    Returns:
        Minimal control-plane configuration.

    Raises:
        ConfigError: The file cannot be read, or its TOML or `daemon.state_dir`
            is invalid.
    """
    root = _parse_toml(path, 'configuration file')
    daemon = _required_mapping(root, 'daemon', 'configuration')
    return ControlConfig(state_dir=_state_directory(daemon, path))


def source_registration_holdback_secs(group: GroupConfig, source: SourceConfig) -> float:
    """Project a group's holdback into its source registration.

    Eager source registrations use zero to disable strict buffering.
    """
    return 0.0 if group.ordering == 'eager' else _strict_group_holdback_secs(group, source)


def source_state_options(daemon: DaemonConfig, source: SourceConfig) -> _SourceStateOptions:
    """Build source authority limits and durability policy from configuration."""
    return {
        'synchronous': daemon.sqlite_synchronous.upper(),
        'visibility_overlap_secs': source.visibility_overlap_secs,
        'max_open_ranges': source.max_open_ranges,
        'max_group_obligations': daemon.max_group_obligations,
        'max_range_query_pages': source.max_range_query_pages,
        'max_pending_group_obligations': source.max_pending_group_obligations,
        'max_source_late_ids': source.max_source_late_ids,
        'max_ipc_frame_bytes': daemon.max_ipc_frame_bytes,
        'commit_batch_max_bytes': source.commit_batch_max_bytes,
    }


def source_stream_fingerprint(site: SiteConfig, source: SourceConfig) -> str:
    """Return the durable non-secret stream identity for a source in its owning site."""
    credential = site.credentials[source.credentials]
    return stream_fingerprint(
        site.server,
        source.credentials,
        credential.script_name,
    )


def evidence_warnings(
    config: Config,
    evidence: AcceptanceEvidence,
) -> tuple[str, ...]:
    """Describe acceptance evidence for strict sources.

    Args:
        config: Fully resolved daemon configuration.
        evidence: Parsed external acceptance evidence.

    Returns:
        Deterministic warnings for strict sources.
    """
    records = {(record.site, record.source): record for record in evidence.sources}
    strict_sources: list[tuple[str, str]] = []
    for site in config.sites.values():
        for group in site.groups.values():
            pair = (site.name, group.source)
            if group.ordering == 'strict' and pair not in strict_sources:
                strict_sources.append(pair)
    result: list[str] = []
    for site_name, source_name in strict_sources:
        record = records.get((site_name, source_name))
        label = f'{site_name}/{source_name}'
        if record is None:
            result.append(f'missing evidence for strict source {label}')
        elif record.source_fingerprint != source_stream_fingerprint(
            config.sites[site_name], config.sites[site_name].sources[source_name]
        ):
            result.append(f'stale evidence for strict source {label}')
        else:
            result.append(f'strict source {label} observed gap rate {record.observed_gap_rate}')
    return tuple(result)
