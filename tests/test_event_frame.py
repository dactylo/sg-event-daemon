from __future__ import annotations

from types import MappingProxyType

import pytest

from sg_event_daemon._event_frame import encode_event_header, event_frame_bytes


def test_event_frame_materializes_only_the_top_level_metadata_mapping() -> None:
    metadata: dict[str, object] = {'event_id': 17, 'replay': False}
    readonly_metadata = MappingProxyType(metadata)

    assert encode_event_header('event', readonly_metadata) == encode_event_header(
        'event', metadata
    )
    assert event_frame_bytes('event', readonly_metadata) == event_frame_bytes('event', metadata)

    with pytest.raises(TypeError):
        encode_event_header('event', {'nested': MappingProxyType({'value': 1})})
    with pytest.raises(ValueError, match='Out of range float'):
        encode_event_header('event', {'value': float('nan')})
