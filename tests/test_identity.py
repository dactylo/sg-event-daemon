import hashlib
from types import MappingProxyType

import pytest

from sg_event_daemon._identity import canonical_bytes, stream_fingerprint


def test_canonical_bytes_materializes_only_the_top_level_mapping() -> None:
    payload: dict[str, object] = {'enabled': True, 'name': 'editorial', 'ordinal': 7}

    assert canonical_bytes(MappingProxyType(payload)) == canonical_bytes(payload)

    with pytest.raises(TypeError):
        canonical_bytes({'nested': MappingProxyType({'value': 1})})
    with pytest.raises(ValueError, match='Out of range float'):
        canonical_bytes({'value': float('nan')})


def test_canonical_bytes_preserves_top_level_sequence_encoding() -> None:
    payload = [['Shotgun_Shot_Change', ['sg_status_list']]]

    assert canonical_bytes(payload) == b'[["Shotgun_Shot_Change",["sg_status_list"]]]'


def test_stream_fingerprint_matches_frozen_canonical_identity() -> None:
    server_url = 'https://studio.example.invalid'
    credential_name = 'admin'
    script_name = 'event-reader'
    canonical = (
        f'{{"credential_name":"{credential_name}",'
        f'"script_name":"{script_name}",'
        f'"server_url":"{server_url}"}}'
    ).encode()

    assert canonical == (
        b'{"credential_name":"admin","script_name":"event-reader",'
        b'"server_url":"https://studio.example.invalid"}'
    )
    expected = hashlib.sha256(canonical).hexdigest()
    assert expected == '4f42e1ece712302f300036bba17629c7452aa1d1cd0a80d5984a8c545251804b'
    assert stream_fingerprint(server_url, credential_name, script_name) == expected
    assert (
        stream_fingerprint(
            'https://archive.example.invalid',
            credential_name,
            script_name,
        )
        != expected
    )
    assert stream_fingerprint(server_url, 'restricted', script_name) != expected
    assert stream_fingerprint(server_url, credential_name, 'audit-reader') != expected
