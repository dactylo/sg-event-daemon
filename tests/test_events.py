from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

import pytest

from sg_event_daemon._events import (
    EVENT_FIELDS,
    Event,
    _EventEncodingError,
    decode_event,
    encode_event,
    matches,
    normalize_filter,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

_EXPECTED_MINIMAL_ENCODING = (
    b'{"event":["map",[[["str","attribute_name"],["null"]],'
    b'[["str","created_at"],["datetime","2026-07-25T01:02:03"]],'
    b'[["str","entity"],["null"]],[["str","event_type"],["str","Shotgun_Task_Change"]],'
    b'[["str","id"],["int",1]],[["str","meta"],["map",[]]],'
    b'[["str","project"],["null"]],[["str","session_uuid"],["null"]],'
    b'[["str","type"],["str","EventLogEntry"]],[["str","user"],["null"]]]],'
    b'"schema":"sg-event-daemon-event","version":1}'
)
_MINIMAL_ENCODING_BYTES = 422
_REQUIRED_EVENT_KEYS = (*EVENT_FIELDS, 'type')


def _event_payload(**changes: object) -> dict[str, object]:
    payload: dict[str, object] = {
        'id': 1,
        'event_type': 'Shotgun_Task_Change',
        'attribute_name': 'sg_status_list',
        'meta': {},
        'entity': None,
        'user': None,
        'project': None,
        'session_uuid': None,
        'created_at': datetime.fromisoformat('2026-07-25T01:02:03'),
        'type': 'EventLogEntry',
    }
    payload.update(changes)
    return payload


def _filter_event(
    *,
    attribute_name: str | None = 'sg_status_list',
    event_type: str = 'Shotgun_Task_Change',
) -> Event:
    return Event(
        _event_payload(
            attribute_name=attribute_name,
            event_type=event_type,
        )
    )


def test_decode_event_normalizes_missing_fields_as_encoding_errors() -> None:
    wire = (
        b'{"event":["map",[[["str","type"],["str","EventLogEntry"]]]],'
        b'"schema":"sg-event-daemon-event","version":1}'
    )

    with pytest.raises(_EventEncodingError, match='missing'):
        decode_event(wire)


@pytest.mark.parametrize(
    ('wire', 'message'),
    [
        (b'\xff', 'UTF-8'),
        (b'{"event":["null"],"schema":"sg-event-daemon-event","version":NaN}', 'finite'),
        (b'{"event":["null"],"schema":"sg-event-daemon-event","version":1.0}', 'version'),
        (b'{"event":["null"],"schema":"sg-event-daemon-event","version":2}', 'version'),
        (
            b'{"event":["null"],"extra":true,"schema":"sg-event-daemon-event","version":1}',
            'envelope',
        ),
        (
            (
                b'{"event":["null"],"schema":"sg-event-daemon-event",'
                b'"schema":"sg-event-daemon-event","version":1}'
            ),
            'duplicate',
        ),
        (
            b'{"event":["pickle","payload"],"schema":"sg-event-daemon-event","version":1}',
            'unknown',
        ),
        (
            b'{"event":["float",Infinity],"schema":"sg-event-daemon-event","version":1}',
            'finite',
        ),
        (
            b'{"event":["bytes","***"],"schema":"sg-event-daemon-event","version":1}',
            'base64',
        ),
        (
            (
                b'{"event":["date","2026-07-25T01:02:03"],'
                b'"schema":"sg-event-daemon-event","version":1}'
            ),
            'date',
        ),
        (
            b'{"event":["datetime","2026-07-25"],"schema":"sg-event-daemon-event","version":1}',
            'datetime',
        ),
    ],
)
def test_decode_event_rejects_invalid_or_unknown_wire_values(wire: bytes, message: str) -> None:
    with pytest.raises(_EventEncodingError, match=message):
        decode_event(wire)


def test_decode_event_requires_a_typed_mapping_event() -> None:
    wire = b'{"event":["str","not a mapping"],"schema":"sg-event-daemon-event","version":1}'

    with pytest.raises(_EventEncodingError, match='mapping'):
        decode_event(wire)


def test_dict_event_returns_a_detached_mutable_top_level_copy() -> None:
    event = Event(
        _event_payload(
            meta={'changes': [{'values': ['ip', 'fin']}]},
            entity={'id': 7, 'name': 'Task 7', 'type': 'Task'},
        )
    )

    copied = dict(event)
    copied['meta'] = {'replacement': True}
    copied['new'] = 'value'

    event_meta = cast('Mapping[str, object]', event['meta'])
    event_changes = cast('tuple[object, ...]', event_meta['changes'])
    event_change = cast('Mapping[str, object]', event_changes[0])
    assert event_change == {'values': ('ip', 'fin')}
    assert cast('Mapping[str, object]', event['entity'])['name'] == 'Task 7'
    assert 'new' not in event
    with pytest.raises(TypeError):
        cast('dict[str, object]', dict(event)['entity'])['name'] = 'Changed'


def test_event_codec_resists_user_mapping_tag_collisions() -> None:
    collision = {
        '__sg_event_datetime__': '2026-07-25T01:02:03',
        'event': ['datetime', '2026-07-25T01:02:03'],
        'schema': 'sg-event-daemon-event',
        'tag': 'bytes',
        'value': 'arbitrary',
        'version': 1,
    }
    event = Event(_event_payload(meta=collision))

    restored = decode_event(encode_event(event))

    assert cast('Mapping[str, object]', restored['meta']) == {
        **collision,
        'event': ('datetime', '2026-07-25T01:02:03'),
    }


def test_event_codec_round_trip_preserves_provider_value_types() -> None:
    aware = datetime(
        2026,
        7,
        25,
        1,
        2,
        3,
        456789,
        tzinfo=timezone(timedelta(hours=-7)),
    )
    event = Event(
        _event_payload(
            created_at=aware,
            entity={'id': 17, 'name': 'Task 17', 'type': 'Task'},
            meta={
                'bool': True,
                'bytes': b'\x00\xff',
                'date': date(2026, 7, 24),
                'datetime': datetime.fromisoformat('2026-07-24T23:59:58.000123'),
                'float': 1.25,
                'int': -3,
                'list': [None, 'text', {'id': 9, 'type': 'Asset'}],
                'tuple': ('a', 'b'),
            },
            project={'id': 4, 'name': 'Project', 'type': 'Project'},
            session_uuid='session',
            user={'id': 5, 'name': 'Artist', 'type': 'HumanUser'},
        )
    )

    restored = decode_event(encode_event(event))

    assert restored == event
    assert restored['created_at'] == aware
    meta = cast('Mapping[str, object]', restored['meta'])
    assert type(meta['date']) is date
    assert type(meta['datetime']) is datetime
    assert meta['bytes'] == b'\x00\xff'
    assert meta['list'] == (None, 'text', {'id': 9, 'type': 'Asset'})
    assert meta['tuple'] == ('a', 'b')


def test_event_copies_and_recursively_freezes_provider_values() -> None:
    source = _event_payload(
        meta={
            'changes': [{'field': 'sg_status_list', 'values': ['ip', 'fin']}],
            'link': {'id': 17, 'name': 'Task 17', 'type': 'Task'},
        }
    )

    event = Event(source)
    source_meta = cast('dict[str, object]', source['meta'])
    source_changes = cast('list[object]', source_meta['changes'])
    cast('dict[str, object]', source_changes[0])['field'] = 'content'
    source_changes.append('later')

    meta = cast('Mapping[str, object]', event['meta'])
    changes = cast('tuple[object, ...]', meta['changes'])
    first_change = cast('Mapping[str, object]', changes[0])
    link = cast('Mapping[str, object]', meta['link'])
    assert isinstance(meta, MappingProxyType)
    assert isinstance(changes, tuple)
    assert isinstance(first_change, MappingProxyType)
    assert isinstance(link, MappingProxyType)
    assert first_change['field'] == 'sg_status_list'
    assert changes == ({'field': 'sg_status_list', 'values': ('ip', 'fin')},)
    with pytest.raises(TypeError):
        cast('dict[str, object]', meta)['new'] = 'value'
    with pytest.raises(TypeError):
        cast('dict[str, object]', first_change)['field'] = 'content'


def test_event_encoding_is_canonical_and_has_a_deterministic_byte_count() -> None:
    payload = _event_payload(
        attribute_name=None,
        meta={},
    )
    reordered = {key: payload[key] for key in reversed(tuple(payload))}

    first = encode_event(Event(payload))
    second = encode_event(Event(reordered))

    assert first == _EXPECTED_MINIMAL_ENCODING
    assert second == _EXPECTED_MINIMAL_ENCODING
    assert len(first) == _MINIMAL_ENCODING_BYTES


def test_event_fields_match_the_provider_query_contract() -> None:
    assert EVENT_FIELDS == (
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


@pytest.mark.parametrize('missing_key', _REQUIRED_EVENT_KEYS)
def test_event_rejects_each_missing_provider_field(missing_key: str) -> None:
    payload = _event_payload()
    del payload[missing_key]

    with pytest.raises(ValueError, match=rf'missing.*{missing_key}'):
        Event(payload)


def test_event_rejects_executable_objects() -> None:
    with pytest.raises(_EventEncodingError, match=r'unsupported.*function'):
        Event(_event_payload(meta={'callable': lambda: None}))


@pytest.mark.parametrize('value', [float('inf'), float('-inf'), float('nan')])
def test_event_rejects_nonfinite_numbers(value: float) -> None:
    with pytest.raises(_EventEncodingError, match='finite'):
        Event(_event_payload(meta={'value': value}))


def test_events_compare_by_payload() -> None:
    first = Event(_event_payload(meta={'values': [1, 2]}))
    second = Event(_event_payload(meta={'values': (1, 2)}))

    assert first == second
    assert first == dict(second)
    assert first != Event(_event_payload(id=2))
    assert first != object()


@pytest.mark.parametrize(
    ('filter_value', 'event', 'expected'),
    [
        (None, _filter_event(), True),
        ({}, _filter_event(), True),
        ({'*': None}, _filter_event(), True),
        ({'*': ['*']}, _filter_event(attribute_name=None), True),
        ({'Shotgun_Task_Change': None}, _filter_event(attribute_name=None), True),
        (
            {'Shotgun_Task_Change': ['content', 'sg_status_list']},
            _filter_event(),
            True,
        ),
        (
            {'Shotgun_Task_Change': ['content']},
            _filter_event(),
            False,
        ),
        (
            {'Shotgun_Asset_Change': None},
            _filter_event(),
            False,
        ),
        (
            {'Shotgun_Task_Change': ['sg_status_list']},
            _filter_event(attribute_name=''),
            False,
        ),
        (
            {'Shotgun_Task_Change': ['*']},
            _filter_event(attribute_name=''),
            True,
        ),
    ],
)
def test_filter_matching(
    filter_value: Mapping[str, object] | None,
    event: Event,
    expected: bool,
) -> None:
    assert matches(normalize_filter(filter_value), event) is expected


@pytest.mark.parametrize(
    ('filter_value', 'expected'),
    [
        ({'*': None, 'Shotgun_Task_Change': []}, True),
        ({'*': ['sg_status_list'], 'Shotgun_Task_Change': ['content']}, True),
        ({'*': ['content'], 'Shotgun_Task_Change': ['sg_status_list']}, True),
        ({'*': ['content'], 'Shotgun_Task_Change': ['description']}, False),
    ],
)
def test_filter_matching_unions_wildcard_and_exact_clauses(
    filter_value: Mapping[str, object],
    expected: bool,
) -> None:
    assert matches(normalize_filter(filter_value), _filter_event()) is expected


def test_normalize_filter_sorts_only_semantically_unordered_values() -> None:
    raw = {
        'Shotgun_Task_Change': ['sg_status_list', 'content', 'sg_status_list'],
        '*': {'project', 'entity'},
        'Shotgun_Asset_New': None,
    }

    normalized = normalize_filter(raw)

    assert normalized == (
        ('*', ('entity', 'project')),
        ('Shotgun_Asset_New', None),
        ('Shotgun_Task_Change', ('content', 'sg_status_list')),
    )
    assert raw['Shotgun_Task_Change'] == [
        'sg_status_list',
        'content',
        'sg_status_list',
    ]
