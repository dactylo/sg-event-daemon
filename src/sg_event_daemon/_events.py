"""Immutable event values, safe wire encoding, and normalized filters."""

from __future__ import annotations

import base64
import binascii
import json
import math
from collections.abc import Collection, Iterator, Mapping
from datetime import date, datetime
from types import MappingProxyType
from typing import TypeAlias, cast

EVENT_FIELDS = (
    'id',
    'event_type',
    'attribute_name',
    'meta',
    'entity',
    'user',
    'project',
    'session_uuid',
    'created_at',
)
_CODEC_SCHEMA = 'sg-event-daemon-event'
_CODEC_VERSION = 1
_NODE_SIZE = 2
_REQUIRED_EVENT_FIELDS = frozenset((*EVENT_FIELDS, 'type'))

EventFilter: TypeAlias = tuple[tuple[str, tuple[str, ...] | None], ...]


class Event(Mapping[str, object]):
    """Expose one copied, recursively immutable provider event mapping."""

    _payload: Mapping[str, object]

    __hash__ = None
    __slots__ = ('_payload',)

    def __init__(self, payload: Mapping[str, object]) -> None:
        """Copy and validate one source-visible provider mapping.

        Args:
            payload: EventLogEntry mapping containing every `EVENT_FIELDS` key
                and `type`. Values may be `None`, strings, booleans, integers,
                finite floats, bytes, dates, datetimes, string-keyed mappings,
                lists, or tuples; nested containers are copied and frozen.

        Raises:
            ValueError: The payload contains an unsupported provider value or a
                required EventLogEntry field is absent.
        """
        frozen = cast('Mapping[str, object]', _freeze(payload))
        missing = sorted(_REQUIRED_EVENT_FIELDS - frozen.keys())
        if missing:
            raise ValueError(f'event has missing required fields: {", ".join(missing)}')
        object.__setattr__(self, '_payload', frozen)

    def __eq__(self, other: object) -> bool:
        """Compare events and mappings by payload value."""
        if isinstance(other, Event):
            return self._payload == other._payload
        if isinstance(other, Mapping):
            return self._payload == other
        return NotImplemented

    def __getitem__(self, key: str) -> object:
        """Return one recursively immutable provider value."""
        return self._payload[key]

    def __iter__(self) -> Iterator[str]:
        """Iterate over provider field names."""
        return iter(self._payload)

    def __len__(self) -> int:
        """Return the number of provider fields."""
        return len(self._payload)

    def __repr__(self) -> str:
        """Return a payload-oriented representation."""
        return f'{type(self).__name__}({dict(self)!r})'

    def __setattr__(self, name: str, value: object) -> None:
        """Reject mutation after construction."""
        raise AttributeError(f'{type(self).__name__}.{name} is immutable')


class _EventEncodingError(ValueError):
    """Report an unsupported event value or invalid event frame."""


def _decode_base64(value: object) -> bytes:
    """Decode one canonical base64 value."""
    if not isinstance(value, str):
        raise _EventEncodingError('bytes node payload must be a base64 string')
    try:
        encoded = value.encode('ascii')
        decoded = base64.b64decode(encoded, validate=True)
    except (UnicodeEncodeError, binascii.Error) as error:
        raise _EventEncodingError('bytes node contains invalid base64') from error
    if base64.b64encode(decoded) != encoded:
        raise _EventEncodingError('bytes node contains non-canonical base64')
    return decoded


def _decode_date(value: object) -> date:
    """Decode one canonical ISO date."""
    if not isinstance(value, str):
        raise _EventEncodingError('date node payload must be a string')
    try:
        decoded = date.fromisoformat(value)
    except ValueError as error:
        raise _EventEncodingError('date node contains an invalid ISO date') from error
    if decoded.isoformat() != value:
        raise _EventEncodingError('date node contains a non-canonical ISO date')
    return decoded


def _decode_datetime(value: object) -> datetime:
    """Decode one canonical ISO datetime."""
    if not isinstance(value, str) or 'T' not in value:
        raise _EventEncodingError('datetime node contains an invalid ISO datetime')
    try:
        decoded = datetime.fromisoformat(value)
    except ValueError as error:
        raise _EventEncodingError('datetime node contains an invalid ISO datetime') from error
    if decoded.isoformat() != value:
        raise _EventEncodingError('datetime node contains a non-canonical ISO datetime')
    return decoded


def _decode_map(value: object) -> dict[str, object]:
    """Decode one closed mapping node payload."""
    if not isinstance(value, list):
        raise _EventEncodingError('map node payload must be an array')
    decoded: dict[str, object] = {}
    for entry in cast('list[object]', value):
        if not isinstance(entry, list):
            raise _EventEncodingError('map node entries must be two-item arrays')
        raw_entry = cast('list[object]', entry)
        if len(raw_entry) != _NODE_SIZE:
            raise _EventEncodingError('map node entries must be two-item arrays')
        key = _decode_node(raw_entry[0])
        if not isinstance(key, str):
            raise _EventEncodingError('map node keys must be string nodes')
        if key in decoded:
            raise _EventEncodingError(f'map node has duplicate key: {key}')
        decoded[key] = _decode_node(raw_entry[1])
    return decoded


def _decode_numeric_node(tag: str, payload: object) -> object:
    """Decode one boolean, integer, or finite floating-point node."""
    if tag == 'bool':
        if not isinstance(payload, bool):
            raise _EventEncodingError('bool node payload must be a boolean')
        decoded = payload
    elif tag == 'float':
        if not isinstance(payload, float) or not math.isfinite(payload):
            raise _EventEncodingError('float node payload must be a finite JSON float')
        decoded = payload
    else:
        if isinstance(payload, bool) or not isinstance(payload, int):
            raise _EventEncodingError('int node payload must be a JSON integer')
        decoded = payload
    return decoded


def _decode_text_node(tag: str, payload: object) -> object:
    """Decode one byte, date, datetime, or string node."""
    if tag == 'bytes':
        decoded = _decode_base64(payload)
    elif tag == 'date':
        decoded = _decode_date(payload)
    elif tag == 'datetime':
        decoded = _decode_datetime(payload)
    else:
        if not isinstance(payload, str):
            raise _EventEncodingError('str node payload must be a string')
        decoded = payload
    return decoded


def _decode_node(value: object) -> object:
    """Decode one value from the closed typed-node schema."""
    if not isinstance(value, list) or not value or not isinstance(value[0], str):
        raise _EventEncodingError('wire value must be a typed array node')
    node = cast('list[object]', value)
    tag = cast('str', node[0])
    if tag == 'null':
        if len(node) != 1:
            raise _EventEncodingError('null node must contain only its tag')
        return None
    if len(node) != _NODE_SIZE:
        raise _EventEncodingError(f'{tag} node must contain a tag and payload')
    payload = node[1]
    if tag == 'array':
        if not isinstance(payload, list):
            raise _EventEncodingError('array node payload must be an array')
        return [_decode_node(item) for item in cast('list[object]', payload)]
    if tag == 'map':
        return _decode_map(payload)
    if tag in {'bool', 'float', 'int'}:
        return _decode_numeric_node(tag, payload)
    if tag in {'bytes', 'date', 'datetime', 'str'}:
        return _decode_text_node(tag, payload)
    raise _EventEncodingError(f'unknown wire value type: {tag}')


def _encode_scalar_node(value: object) -> list[object] | None:
    """Encode one scalar provider value, if supported."""
    if value is None:
        node: list[object] | None = ['null']
    elif isinstance(value, bool):
        node = ['bool', value]
    elif isinstance(value, int):
        node = ['int', value]
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise _EventEncodingError('event values must contain only finite numbers')
        node = ['float', value]
    elif isinstance(value, str):
        node = ['str', value]
    elif isinstance(value, bytes):
        node = ['bytes', base64.b64encode(value).decode('ascii')]
    elif isinstance(value, datetime):
        node = ['datetime', value.isoformat()]
    elif isinstance(value, date):
        node = ['date', value.isoformat()]
    else:
        node = None
    return node


def _encode_node(value: object) -> list[object]:
    """Encode one provider value as a typed array node."""
    scalar = _encode_scalar_node(value)
    if scalar is not None:
        return scalar
    if isinstance(value, Mapping):
        mapping = cast('Mapping[object, object]', value)
        if not all(isinstance(key, str) for key in mapping):
            raise _EventEncodingError('event mappings must contain only string keys')
        string_mapping = cast('Mapping[str, object]', mapping)
        entries = [
            [_encode_node(key), _encode_node(string_mapping[key])]
            for key in sorted(string_mapping)
        ]
        return ['map', entries]
    if isinstance(value, tuple | list):
        return ['array', [_encode_node(item) for item in cast('Collection[object]', value)]]
    raise _EventEncodingError(f'unsupported event value type: {type(value).__name__}')


def _freeze(value: object) -> object:
    """Copy, validate, and recursively freeze one provider value."""
    if value is None or isinstance(value, str | bool | int | bytes | date):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _EventEncodingError('event values must contain only finite numbers')
        return value
    if isinstance(value, Mapping):
        mapping = cast('Mapping[object, object]', value)
        if not all(isinstance(key, str) for key in mapping):
            raise _EventEncodingError('event mappings must contain only string keys')
        return MappingProxyType({cast('str', key): _freeze(item) for key, item in mapping.items()})
    if isinstance(value, tuple | list):
        return tuple(_freeze(item) for item in cast('Collection[object]', value))
    raise _EventEncodingError(f'unsupported event value type: {type(value).__name__}')


def _json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Build a JSON object while rejecting duplicate keys."""
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise _EventEncodingError(f'JSON object has duplicate key: {key}')
        value[key] = item
    return value


def _matches_attributes(attributes: tuple[str, ...] | None, attribute_name: object) -> bool:
    """Return whether one applicable filter clause matches an attribute."""
    if attributes is None or '*' in attributes:
        return True
    return (
        isinstance(attribute_name, str) and bool(attribute_name) and attribute_name in attributes
    )


def _reject_json_constant(value: str) -> object:
    """Reject non-finite JSON number constants."""
    raise _EventEncodingError(f'JSON number must be finite: {value}')


def _load_envelope(frame: bytes) -> dict[str, object]:
    """Load and validate one strict wire envelope."""
    try:
        text = frame.decode('utf-8', errors='strict')
    except UnicodeDecodeError as error:
        raise _EventEncodingError('event frame must be strict UTF-8') from error
    try:
        raw = cast(
            'object',
            json.loads(
                text,
                object_pairs_hook=_json_object,
                parse_constant=_reject_json_constant,
            ),
        )
    except _EventEncodingError:
        raise
    except (json.JSONDecodeError, TypeError, ValueError) as error:
        raise _EventEncodingError(f'invalid event JSON: {error}') from error
    if not isinstance(raw, dict):
        raise _EventEncodingError('event frame has an invalid envelope')
    envelope = cast('dict[str, object]', raw)
    if set(envelope) != {'event', 'schema', 'version'}:
        raise _EventEncodingError('event frame has an invalid envelope')
    if envelope['schema'] != _CODEC_SCHEMA:
        raise _EventEncodingError('event frame has an unknown schema')
    version = envelope['version']
    if isinstance(version, bool) or not isinstance(version, int) or version != _CODEC_VERSION:
        raise _EventEncodingError('event frame has an unsupported version')
    return envelope


def decode_event(frame: bytes) -> Event:
    """Decode one event from the closed ephemeral wire schema.

    Args:
        frame: Strict UTF-8 JSON bytes containing one versioned event envelope.

    Returns:
        The decoded immutable event.

    Raises:
        ValueError: The frame is not valid strict wire data.
    """
    envelope = _load_envelope(frame)
    payload = _decode_node(envelope['event'])
    if not isinstance(payload, dict):
        raise _EventEncodingError('decoded event must be a mapping')
    try:
        return Event(cast('dict[str, object]', payload))
    except _EventEncodingError:
        raise
    except ValueError as error:
        raise _EventEncodingError(f'decoded event is invalid: {error}') from error


def encode_event(event: Event) -> bytes:
    """Encode one event as canonical strict UTF-8 JSON.

    Args:
        event: Immutable provider event to encode.

    Returns:
        Deterministic bytes in the internal ephemeral wire format.

    Raises:
        ValueError: The event contains an unsupported wire value.
    """
    envelope = {
        'event': _encode_node(event),
        'schema': _CODEC_SCHEMA,
        'version': _CODEC_VERSION,
    }
    try:
        return json.dumps(
            envelope,
            allow_nan=False,
            ensure_ascii=False,
            separators=(',', ':'),
            sort_keys=True,
        ).encode('utf-8', errors='strict')
    except (TypeError, UnicodeEncodeError, ValueError) as error:
        raise _EventEncodingError(f'event cannot be encoded: {error}') from error


def matches(filter_: EventFilter | None, event: Event) -> bool:
    """Return whether an event satisfies a normalized filter.

    Args:
        filter_: Normalized callback filter, or `None` for no filter.
        event: Event to test.

    Returns:
        `True` when the event should be delivered.
    """
    if not filter_:
        return True
    filters = dict(filter_)
    event_type = event['event_type']
    if not isinstance(event_type, str):
        return False
    attribute_name = event['attribute_name']
    return any(
        _matches_attributes(filters[clause], attribute_name)
        for clause in ('*', event_type)
        if clause in filters
    )


def normalize_filter(filter_: object | None) -> EventFilter | None:
    """Validate and canonicalize one callback filter.

    Args:
        filter_: Mapping of nonempty event-type names to `None` or finite
            collections of nonempty attribute names. Strings, bytes, and
            mappings are not attribute collections. `None` or an empty mapping
            matches every event. `*` matches every event type or attribute;
            `None` matches every attribute of that event type, while an empty
            attribute collection matches none.

    Returns:
        `None` when `filter_` is `None`; otherwise a tuple sorted by event type
        with deduplicated, sorted attribute fields. An empty mapping produces
        an empty tuple.

    Raises:
        TypeError: A filter key or field collection has an invalid type.
        ValueError: An event type or attribute name is empty.
    """
    if filter_ is None:
        return None
    if not isinstance(filter_, Mapping):
        raise TypeError('event filter must be a mapping or None')
    normalized: list[tuple[str, tuple[str, ...] | None]] = []
    mapping = cast('Mapping[object, object]', filter_)
    for event_type, raw_attributes in mapping.items():
        if not isinstance(event_type, str):
            raise TypeError('event filter types must be strings')
        if not event_type:
            raise ValueError('event filter types must not be empty')
        if raw_attributes is None:
            normalized.append((event_type, None))
            continue
        if isinstance(raw_attributes, str | bytes | Mapping) or not isinstance(
            raw_attributes, Collection
        ):
            raise TypeError('event filter attributes must be a field collection or None')
        attributes = cast('Collection[object]', raw_attributes)
        if not all(isinstance(attribute, str) for attribute in attributes):
            raise TypeError('event filter attributes must be strings')
        string_attributes = cast('Collection[str]', attributes)
        if any(not attribute for attribute in string_attributes):
            raise ValueError('event filter attributes must not be empty')
        normalized.append((event_type, tuple(sorted(set(string_attributes)))))
    return tuple(sorted(normalized))
