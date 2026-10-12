"""Canonical non-secret daemon identities and digests."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import cast


def canonical_bytes(payload: object) -> bytes:
    """Return deterministic compact UTF-8 JSON bytes.

    Args:
        payload: JSON-compatible canonical payload. Top-level mappings are
            materialized before encoding.

    Returns:
        Sorted compact UTF-8 bytes.
    """
    canonical = (
        dict(cast('Mapping[object, object]', payload)) if isinstance(payload, Mapping) else payload
    )
    return json.dumps(
        canonical,
        allow_nan=False,
        ensure_ascii=False,
        separators=(',', ':'),
        sort_keys=True,
    ).encode()


def digest(payload: bytes) -> str:
    """Return a lowercase hexadecimal SHA-256 digest.

    Args:
        payload: Exact bytes to hash.

    Returns:
        Lowercase hexadecimal SHA-256.
    """
    return hashlib.sha256(payload).hexdigest()


def stream_fingerprint(
    server_url: str,
    credential_name: str,
    script_name: str,
) -> str:
    """Return the canonical non-secret source stream fingerprint.

    Args:
        server_url: Configured SG server URL.
        credential_name: Stable configured credential identity.
        script_name: SG script name used by the credential.

    Returns:
        Lowercase SHA-256 over the exact non-secret stream identity.
    """
    return digest(
        canonical_bytes(
            {
                'credential_name': credential_name,
                'script_name': script_name,
                'server_url': server_url,
            }
        )
    )
