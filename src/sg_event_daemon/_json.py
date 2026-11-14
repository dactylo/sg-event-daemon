"""Strict JSON parsing shared by event and control boundaries."""

from __future__ import annotations

import json
from typing import cast


def _object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate keys before constructing an object."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate JSON key: {key}')
        result[key] = value
    return result


def _reject_constant(value: str) -> object:
    """Reject JavaScript non-finite constants outside the JSON grammar."""
    raise ValueError(f'non-finite JSON number: {value}')


def decode_json(body: bytes) -> object:
    """Parse strict UTF-8 JSON, rejecting duplicate keys and non-finite constants.

    Raises:
        ValueError: The bytes are not strict UTF-8 JSON or contain duplicate
            object keys or non-finite constants.
    """
    try:
        text = body.decode('utf-8', errors='strict')
    except UnicodeDecodeError as error:
        raise ValueError('JSON must be strict UTF-8') from error
    return cast(
        'object',
        json.loads(
            text,
            object_pairs_hook=_object_pairs,
            parse_constant=_reject_constant,
        ),
    )
