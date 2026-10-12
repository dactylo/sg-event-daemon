"""Canonical event-channel headers and exact frame sizing."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

EVENT_SCHEMA = 'sg-event-daemon-event-channel'
EVENT_VERSION = 1
_SEGMENTED_FRAME_PREFIX_BYTES = 8


def encode_event_header(kind: str, metadata: Mapping[str, object]) -> bytes:
    """Encode one canonical event-channel header.

    Args:
        kind: Closed event-channel message kind.
        metadata: JSON-native message metadata.

    Returns:
        Sorted compact UTF-8 header bytes for protocol version one.
    """
    return json.dumps(
        {
            'kind': kind,
            'metadata': dict(metadata),
            'schema': EVENT_SCHEMA,
            'version': EVENT_VERSION,
        },
        allow_nan=False,
        ensure_ascii=False,
        separators=(',', ':'),
        sort_keys=True,
    ).encode('utf-8', errors='strict')


def event_frame_bytes(
    kind: str,
    metadata: Mapping[str, object],
    *,
    payload_byte_count: int = 0,
) -> int:
    """Return one complete segmented event-channel frame size.

    Args:
        kind: Closed event-channel message kind.
        metadata: JSON-native message metadata.
        payload_byte_count: Encoded payload bytes after the header.

    Returns:
        Complete bytes including both four-byte prefix words.
    """
    return (
        _SEGMENTED_FRAME_PREFIX_BYTES
        + len(encode_event_header(kind, metadata))
        + payload_byte_count
    )
