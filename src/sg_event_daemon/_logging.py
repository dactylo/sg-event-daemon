"""Daemon-owned stdout logging and recursive secret redaction."""

from __future__ import annotations

import logging
import sys
import threading
from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import fields, is_dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal, Protocol, assert_type, cast, overload

from ._config import SecretValue

if TYPE_CHECKING:
    from typing import IO

_DISPATCH_FIELDS = (
    'site',
    'source',
    'group',
    'plugin',
    'callback',
    'event_id',
    'execution_mode',
    'delivery_attempt',
    'is_operator_replay',
    'duration',
    'outcome',
)
_ordered_redaction_values: tuple[str, ...] = ()
_REDACTED = '<redacted>'
_REDACTION_LOCK = threading.Lock()
_REDACTION_VALUES: set[str] = set()
SENSITIVE_NAMES = frozenset(
    {
        'api_key',
        'apikey',
        'auth',
        'auth_header',
        'auth_headers',
        'authorization',
        'credential',
        'credentials',
        'password',
        'secret',
        'secrets',
        'token',
    }
)
_STANDARD_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__)


class _StringPreservingTransform(Protocol):
    """Transform arbitrary values while preserving string results."""

    @overload
    def __call__(self, value: str, /) -> str: ...

    @overload
    def __call__(self, value: object, /) -> object: ...


class _TextFormatter(logging.Formatter):
    """Format one readable daemon record with dispatch context."""

    def format(self, record: logging.LogRecord) -> str:
        """Format one sanitized log record.

        Args:
            record: Sanitized daemon record.

        Returns:
            One readable line and optional exception traceback.
        """
        timestamp = datetime.fromtimestamp(record.created, UTC).isoformat()
        context = ' '.join(
            f'{name}={_redact_value(value)}'
            for name in _DISPATCH_FIELDS
            if (value := getattr(record, name, None)) is not None
        )
        line = f'{timestamp} {record.levelname} {record.name}: {redact_text(record.getMessage())}'
        if context:
            line = f'{line} {context}'
        if (exception := _format_exception(self, record)) is not None:
            line = f'{line}\n{exception}'
        return line


class _RedactionFilter(logging.Filter):
    """Redact messages, arguments, and extras before formatting."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact one mutable daemon record.

        Args:
            record: Record emitted under the daemon logger hierarchy.

        Returns:
            Always `True` so the sanitized record reaches its formatter.
        """
        record.msg = _redact_value(record.msg)
        record.args = _redact_args(record.args)
        for name, value in tuple(record.__dict__.items()):
            if name not in _STANDARD_RECORD_FIELDS and name != 'exc_info':
                record.__dict__[name] = _redact_value(value)
        return True


def _collect_children(
    value: object,
    *,
    sensitive: bool,
    seen: set[int],
) -> set[str]:
    """Collect secrets from one structured object's public children."""
    if isinstance(value, Mapping):
        mapping = cast('Mapping[object, object]', value)
        return _collect_named_children(
            mapping.items(),
            sensitive=sensitive,
            seen=seen,
        )
    if isinstance(value, Collection):
        result: set[str] = set()
        for item in cast('Collection[object]', value):
            result.update(_collect_strings(item, sensitive=sensitive, seen=seen))
        return result
    if is_dataclass(value) and not isinstance(value, type):
        return _collect_named_children(
            (
                (field.name, getattr(value, field.name))
                for field in fields(value)
                if not field.name.startswith('_')
            ),
            sensitive=False,
            seen=seen,
        )
    attributes = getattr(value, '__dict__', None)
    if isinstance(attributes, dict):
        return _collect_named_children(
            (
                (name, item)
                for name, item in cast('dict[str, object]', attributes).items()
                if not name.startswith('_')
            ),
            sensitive=sensitive,
            seen=seen,
        )
    return set()


def _collect_named_children(
    children: Iterable[tuple[object, object]],
    *,
    sensitive: bool,
    seen: set[int],
) -> set[str]:
    """Collect secrets from public name-value children."""
    result: set[str] = set()
    for name, item in children:
        normalized = str(name).casefold().replace('-', '_')
        result.update(
            _collect_strings(
                item,
                sensitive=sensitive or normalized in SENSITIVE_NAMES,
                seen=seen,
            )
        )
    return result


def _collect_strings(
    value: object,
    *,
    sensitive: bool,
    seen: set[int],
) -> set[str]:
    """Collect secret values from recursively inspected public objects."""
    if value is None or isinstance(value, bool | int | float):
        return set()
    if isinstance(value, SecretValue):
        revealed = value.reveal()
        return {revealed} if revealed else set()
    if isinstance(value, str | bytes):
        text = value if isinstance(value, str) else value.decode(errors='replace')
        return {text} if sensitive and text else set()
    identity = id(value)
    if identity in seen:
        return set()
    seen.add(identity)
    return _collect_children(value, sensitive=sensitive, seen=seen)


def _discover_redaction_values(objects: Iterable[object]) -> set[str]:
    """Discover plaintext secrets in public configuration and runtime objects."""
    found: set[str] = set()
    for value in objects:
        found.update(_collect_strings(value, sensitive=False, seen=set()))
    return found


def _reconstruct_log_container(
    value: object,
    leaf: _StringPreservingTransform,
    visit: Callable[[object], object],
    *,
    mode: Literal['daemon', 'plugin'],
) -> object:
    """Reconstruct one supported container with transformed children."""
    if isinstance(value, Mapping):
        mapping = cast('Mapping[object, object]', value)
        if mode == 'daemon':
            return {leaf(str(key)): visit(item) for key, item in mapping.items()}
        return {visit(key): visit(item) for key, item in mapping.items()}
    if isinstance(value, tuple):
        items = cast('tuple[object, ...]', value)
        return tuple(visit(item) for item in items)
    if isinstance(value, list):
        items = cast('list[object]', value)
        return [visit(item) for item in items]
    if mode == 'plugin':
        if isinstance(value, set):
            items = cast('set[object]', value)
            reconstructed: object = {visit(item) for item in items}
        else:
            items = cast('frozenset[object]', value)
            reconstructed = frozenset(visit(item) for item in items)
        return reconstructed
    items = cast('Collection[object]', value)
    return [visit(item) for item in items]


def configure_logging(
    *,
    level: int | str = logging.INFO,
    redact_from: Iterable[object] = (),
    stream: IO[str] | None = None,
) -> logging.Logger:
    """Configure the nonpropagating daemon stdout logger hierarchy.

    Clear and repopulate the daemon redaction registry from `redact_from`.
    Close and replace the handlers on the `sg_event_daemon` root logger.

    Args:
        level: Standard logging level or level name.
        redact_from: Public configuration and runtime objects to inspect.
        stream: Optional text stream; defaults to current standard output.

    Returns:
        The configured `sg_event_daemon` hierarchy root.
    """
    global _ordered_redaction_values  # noqa: PLW0603

    found = _discover_redaction_values(redact_from)
    with _REDACTION_LOCK:
        _REDACTION_VALUES.clear()
        _REDACTION_VALUES.update(found)
        _ordered_redaction_values = tuple(sorted(_REDACTION_VALUES, key=len, reverse=True))
    logger = logging.getLogger('sg_event_daemon')
    for existing in logger.handlers:
        existing.close()
    handler = logging.StreamHandler(sys.stdout if stream is None else stream)
    handler.addFilter(_RedactionFilter())
    handler.setFormatter(_TextFormatter())
    logger.handlers[:] = [handler]
    logger.setLevel(level)
    logger.propagate = False
    return logger


def redact_text(value: str) -> str:
    """Replace every daemon-registered plaintext value in one string.

    Args:
        value: Text that may contain a registered plaintext value.

    Returns:
        Text with every registered value replaced by the redaction marker.
    """
    with _REDACTION_LOCK:
        values = _ordered_redaction_values
    result = value
    for secret in values:
        result = result.replace(secret, _REDACTED)
    return result


def _format_exception(formatter: logging.Formatter, record: logging.LogRecord) -> str | None:
    """Render and redact one original or pre-rendered exception."""
    if record.exc_info is not None:
        return redact_text(formatter.formatException(record.exc_info))
    if record.exc_text is not None:
        return redact_text(record.exc_text)
    return None


@overload
def _redact_scalar(value: str, /) -> str: ...


@overload
def _redact_scalar(value: object, /) -> object: ...


def _redact_scalar(value: object) -> object:
    """Redact one scalar or safely stringify an opaque value."""
    if value is None or isinstance(value, bool | int | float):
        return value
    try:
        return redact_text(str(value))
    except Exception:  # noqa: BLE001
        return '<unprintable>'


def register_redaction_values(*objects: object) -> None:
    """Register secrets discovered recursively in public objects.

    Args:
        *objects: Resolved configuration and runtime objects to inspect.
    """
    global _ordered_redaction_values  # noqa: PLW0603

    found = _discover_redaction_values(objects)
    with _REDACTION_LOCK:
        if not found.issubset(_REDACTION_VALUES):
            _REDACTION_VALUES.update(found)
            _ordered_redaction_values = tuple(sorted(_REDACTION_VALUES, key=len, reverse=True))


def transform_log_value(
    value: object,
    leaf: _StringPreservingTransform,
    *,
    mode: Literal['daemon', 'plugin'],
) -> object:
    """Transform one recursive log value with route-specific reconstruction.

    Containers are copied, and active reference cycles become
    `<redacted-cycle>`. Bytes are decoded as UTF-8 with replacement before
    applying `leaf`; plugin mode re-encodes the transformed text.

    Args:
        value: Root logging value to transform.
        leaf: Context-specific scalar renderer and text replacer.
        mode: `daemon` transforms all mappings and collections, renders mapping
            keys as text, preserves tuples, and turns other collections into
            lists. `plugin` recursively transforms mappings, tuples, lists,
            sets, and frozensets, preserving those sequence or set families and
            transforming keys recursively.

    Returns:
        A recursively reconstructed value with transformed scalar leaves.
    """
    active: set[int] = set()

    def _visit(item: object) -> object:
        if isinstance(item, str):
            return assert_type(leaf(item), str)
        if isinstance(item, bytes):
            transformed = assert_type(leaf(item.decode(errors='replace')), str)
            return transformed if mode == 'daemon' else transformed.encode()
        if mode == 'daemon':
            is_container = isinstance(item, Mapping | Collection)
        else:
            is_container = isinstance(item, Mapping | tuple | list | set | frozenset)
        if not is_container:
            return leaf(item)

        identity = id(item)
        if identity in active:
            return '<redacted-cycle>'
        active.add(identity)
        try:
            return _reconstruct_log_container(item, leaf, _visit, mode=mode)
        finally:
            active.discard(identity)

    return _visit(value)


def _redact_value(value: object) -> object:
    """Recursively copy one value while replacing registered secrets."""
    return transform_log_value(value, _redact_scalar, mode='daemon')


def _redact_arg(value: object) -> object:
    """Redact one interpolation argument without stringifying opaque scalars."""
    if isinstance(value, str | bytes | Mapping | Collection):
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
