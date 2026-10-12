from __future__ import annotations

import hashlib
import json
import pwd
from dataclasses import FrozenInstanceError, replace
from typing import cast

import pytest

import sg_event_daemon._group_types as group_types
from sg_event_daemon._group_types import (
    CatchupEpoch,
    GroupSnapshot,
    LiveJoinIdentity,
    manifest_from_json,
    snapshot_from_json,
    snapshot_to_json,
)
from sg_event_daemon._plugins import CallbackIdentity, CallbackManifest


def _assign_frozen_field(value: object, field: str, replacement: object) -> None:
    setattr(value, field, replacement)


def _attempt_id(label: str) -> str:
    return f'join-attempt-{label}'


def _durable_manifest() -> CallbackManifest:
    return CallbackManifest(
        callbacks=(
            CallbackIdentity(
                ordinal=0,
                plugin_id='publish',
                callback_id='publish-event',
                execution='sync',
                filter_digest='filter-1',
                config_digest='config-1',
                secret_refs=(('api_key', 'SHOTGRID_API_KEY'),),
                credential_name='publisher',
                credential_fingerprint='publisher:script',
            ),
        )
    )


def _durable_snapshot() -> GroupSnapshot:
    return GroupSnapshot(
        site='studio',
        source='events',
        group='editorial',
        group_generation=7,
        revision=9,
        status='callback-failed',
        conditions=('callback-failed',),
        cursor=41,
        delivery_floor=41,
        verified_incarnation='incarnation-1',
        ordering='strict',
        stream_fingerprint='stream-v1',
        source_generation='source-spawn-1',
        in_flight_event_id=42,
        in_flight_obligation_id='range-1',
        manifest=_durable_manifest(),
        required_event_id=42,
        retry_event_id=42,
        callback_marks=((0, 'publish', 'publish-event', 'failed', 1),),
        failure_code='callback-failed',
        failure_text='failure',
        failure_byte_count=7,
        failure_digest='failure-digest',
        failure_ordinal=0,
        parked=True,
        is_operator_replay=False,
        replay_required_from=40,
        history_truncated_floor=1,
        ordering_anomaly_id=43,
        strict_capacity_abort_streak=2,
        strict_capacity_attempt_token=_attempt_id('strict-capacity'),
        reader_clock_checkpoint=100.5,
        latest_join_boundary=44,
        obligation_count=1,
        late_id_count=2,
        epochs=(
            CatchupEpoch(
                epoch_id=1,
                low_id=42,
                high_id=44,
                captured_at=100.0,
                eligibility_point=400.0,
                reader_clock_checkpoint=100.0,
                source_generation='source-spawn-1',
                group_generation=7,
                state='waiting',
            ),
        ),
    )


def _json_with_list_entry(
    encoded: str,
    key: str,
    index: int,
    value: object,
) -> str:
    payload = cast('dict[str, object]', json.loads(encoded))
    items = cast('list[object]', payload[key])
    items[index] = value
    return json.dumps(payload)


def _json_with_nested_field(
    encoded: str,
    list_key: str,
    field: str,
    value: object,
) -> str:
    payload = cast('dict[str, object]', json.loads(encoded))
    items = cast('list[object]', payload[list_key])
    item = cast('dict[str, object]', items[0])
    item[field] = value
    return json.dumps(payload)


def _json_with_number_literal(encoded: str, current: str, replacement: str) -> str:
    assert encoded.count(current) == 1
    return encoded.replace(current, replacement)


def _json_with_root_field(encoded: str, field: str, value: object) -> str:
    payload = cast('dict[str, object]', json.loads(encoded))
    payload[field] = value
    return json.dumps(payload)


def _json_without_nested_field(encoded: str, list_key: str, field: str) -> str:
    payload = cast('dict[str, object]', json.loads(encoded))
    items = cast('list[object]', payload[list_key])
    item = cast('dict[str, object]', items[0])
    item.pop(field)
    return json.dumps(payload)


def _json_without_root_field(encoded: str, field: str) -> str:
    payload = cast('dict[str, object]', json.loads(encoded))
    payload.pop(field)
    return json.dumps(payload)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('evidence_id', ''),
        ('site', 'api_key=hidden'),
        ('group_generation', True),
        ('event_id', -1),
        ('group_revision', True),
        ('observed_at_wall_clock', float('nan')),
        ('observed_at_wall_clock', 10**1000),
    ],
    ids=[
        'empty identity',
        'secret identity',
        'boolean generation',
        'negative event id',
        'boolean revision',
        'nan timestamp',
        'oversized timestamp',
    ],
)
def test_conditional_park_rejects_invalid_values_at_construction(
    field: str, value: object
) -> None:
    directive = group_types.ConditionalParkDirective(
        evidence_id='evidence-1',
        site='studio',
        group='editorial',
        group_generation=7,
        failed_spawn_instance='spawn-1',
        event_id=42,
        group_revision=9,
        observed_at_wall_clock=1000.5,
    )
    with pytest.raises(ValueError, match='conditional park directive'):
        replace(directive, **{field: value})


def test_conditional_park_values_are_immutable_and_fingerprint_every_field() -> None:
    directive = group_types.ConditionalParkDirective(
        evidence_id='evidence-1',
        site='studio',
        group='editorial',
        group_generation=7,
        failed_spawn_instance='spawn-1',
        event_id=42,
        group_revision=9,
        observed_at_wall_clock=1000.5,
    )
    expected = (
        b'{"event_id":42,"evidence_id":"evidence-1",'
        b'"failed_spawn_instance":"spawn-1","group":"editorial",'
        b'"group_generation":7,"group_revision":9,'
        b'"observed_at_wall_clock":1000.5,"site":"studio"}'
    )
    fingerprint = _sha256(expected)

    assert directive.fingerprint() == fingerprint
    for field, replacement in (
        ('evidence_id', 'evidence-2'),
        ('site', 'review'),
        ('group', 'lighting'),
        ('group_generation', 8),
        ('failed_spawn_instance', 'spawn-2'),
        ('event_id', 43),
        ('group_revision', 10),
        ('observed_at_wall_clock', 1001.5),
    ):
        assert replace(directive, **{field: replacement}).fingerprint() != fingerprint

    result = group_types.ConditionalParkResult(
        evidence_id='evidence-1',
        outcome='stale',
        resulting_revision=10,
    )
    with pytest.raises(FrozenInstanceError):
        _assign_frozen_field(directive, 'event_id', 43)
    with pytest.raises(FrozenInstanceError):
        _assign_frozen_field(result, 'outcome', 'parked')


@pytest.mark.parametrize('missing_entry', [False, True])
def test_current_audit_identity_preserves_numeric_identity(
    monkeypatch: pytest.MonkeyPatch, missing_entry: bool
) -> None:
    def lookup(uid: int) -> pwd.struct_passwd:
        assert uid == 12345
        if missing_entry:
            raise KeyError(uid)
        return pwd.struct_passwd(('service', 'x', uid, 100, '', '/', '/bin/false'))

    monkeypatch.setattr(group_types.os, 'geteuid', lambda: 12345)
    monkeypatch.setattr(group_types.os, 'getpid', lambda: 678)
    monkeypatch.setattr(group_types.pwd, 'getpwuid', lookup)

    identity = group_types.AuditIdentity.current()

    assert identity.local_uid == 12345
    assert identity.pid == 678
    assert identity.username == ('12345' if missing_entry else 'service')
    assert tuple(identity.__dataclass_fields__) == ('local_uid', 'username', 'pid')


@pytest.mark.parametrize(
    ('value', 'expected'),
    [('initialize', True), ('resume', True), ('restart', False), ('', False), (None, False)],
)
def test_is_start_mode_accepts_only_supported_strings(value: object, expected: bool) -> None:
    assert group_types.is_start_mode(value) is expected


def test_is_start_mode_rejects_non_string_equality_spoof() -> None:
    class EqualToInitialize:
        def __eq__(self, other: object) -> bool:
            return other == 'initialize'

        def __hash__(self) -> int:
            return hash('initialize')

    value = EqualToInitialize()
    assert value == 'initialize'
    assert not isinstance(value, str)
    assert not group_types.is_start_mode(value)


def test_live_join_identity_projects_the_canonical_wire_payload() -> None:
    identity = LiveJoinIdentity(
        site='studio',
        source='events',
        group='editorial',
        join_kind='strict',
        source_generation='source-1',
        group_generation=7,
        boundary=99,
        attempt_token='a' * 32,
    )

    payload = identity.wire_payload()

    assert payload == {
        'attempt_token': 'a' * 32,
        'boundary': 99,
        'group': 'editorial',
        'group_generation': 7,
        'join_kind': 'strict',
        'site': 'studio',
        'source': 'events',
        'source_generation': 'source-1',
    }
    assert tuple(payload) == (
        'attempt_token',
        'boundary',
        'group',
        'group_generation',
        'join_kind',
        'site',
        'source',
        'source_generation',
    )


def test_manifest_from_json_preserves_canonical_round_trip() -> None:
    manifest = _durable_manifest()

    restored = manifest_from_json(manifest.to_json())

    assert restored == manifest
    assert restored.to_json() == manifest.to_json()


@pytest.mark.parametrize(
    'encoded',
    [
        '{"callbacks":',
        '[]',
        _json_without_root_field(_durable_manifest().to_json(), 'callbacks'),
        _json_with_root_field(_durable_manifest().to_json(), 'unknown', None),
        _json_with_root_field(_durable_manifest().to_json(), 'callbacks', {}),
        _json_with_list_entry(_durable_manifest().to_json(), 'callbacks', 0, []),
        _json_without_nested_field(_durable_manifest().to_json(), 'callbacks', 'plugin_id'),
        _json_with_nested_field(
            _durable_manifest().to_json(),
            'callbacks',
            'unknown',
            None,
        ),
        _json_with_nested_field(
            _durable_manifest().to_json(),
            'callbacks',
            'ordinal',
            value=True,
        ),
        _json_with_nested_field(_durable_manifest().to_json(), 'callbacks', 'plugin_id', 1),
        _json_with_nested_field(
            _durable_manifest().to_json(),
            'callbacks',
            'execution',
            'threaded',
        ),
        _json_with_nested_field(_durable_manifest().to_json(), 'callbacks', 'ordinal', 1),
        _json_with_nested_field(_durable_manifest().to_json(), 'callbacks', 'secret_refs', {}),
        _json_with_nested_field(
            _durable_manifest().to_json(),
            'callbacks',
            'secret_refs',
            [['api_key']],
        ),
        _json_with_nested_field(
            _durable_manifest().to_json(),
            'callbacks',
            'secret_refs',
            [['api_key', 1]],
        ),
    ],
)
def test_manifest_from_json_rejects_durable_json_corruption(encoded: str) -> None:
    with pytest.raises(ValueError, match='invalid callback manifest JSON') as error:
        manifest_from_json(encoded)

    assert error.value.__cause__ is not None


def test_recovery_identity_values_are_immutable_and_complete() -> None:
    epoch = CatchupEpoch(
        epoch_id=3,
        low_id=41,
        high_id=45,
        captured_at=100.0,
        eligibility_point=400.0,
        reader_clock_checkpoint=100.0,
        source_generation='source-spawn-1',
        group_generation=8,
        state='waiting',
    )
    join = LiveJoinIdentity(
        site='studio',
        source='events',
        group='editorial',
        join_kind='strict',
        source_generation='source-spawn-1',
        group_generation=8,
        boundary=45,
        attempt_token=_attempt_id('1'),
    )

    assert epoch.eligibility_point == 400.0
    assert join.attempt_token == _attempt_id('1')
    for value in (epoch, join):
        with pytest.raises(FrozenInstanceError):
            setattr(value, next(iter(value.__dataclass_fields__)), None)


def test_snapshot_and_epoch_constructors_normalize_numeric_fields() -> None:
    integer_snapshot = replace(
        _durable_snapshot(),
        reader_clock_checkpoint=100,
        epochs=(
            CatchupEpoch(
                epoch_id=1,
                low_id=42,
                high_id=44,
                captured_at=100,
                eligibility_point=400,
                reader_clock_checkpoint=100,
                source_generation='source-spawn-1',
                group_generation=7,
                state='waiting',
            ),
        ),
    )
    float_snapshot = replace(
        integer_snapshot,
        reader_clock_checkpoint=100.0,
        epochs=(
            replace(
                integer_snapshot.epochs[0],
                captured_at=100.0,
                eligibility_point=400.0,
                reader_clock_checkpoint=100.0,
            ),
        ),
    )

    assert type(integer_snapshot.reader_clock_checkpoint) is float
    assert type(integer_snapshot.epochs[0].captured_at) is float
    assert type(integer_snapshot.epochs[0].eligibility_point) is float
    assert type(integer_snapshot.epochs[0].reader_clock_checkpoint) is float
    assert snapshot_to_json(integer_snapshot) == snapshot_to_json(float_snapshot)
    assert snapshot_from_json(snapshot_to_json(integer_snapshot)) == integer_snapshot


def test_snapshot_from_json_preserves_canonical_round_trip() -> None:
    snapshot = _durable_snapshot()

    restored = snapshot_from_json(snapshot_to_json(snapshot))

    assert restored == snapshot
    assert snapshot_to_json(restored) == snapshot_to_json(snapshot)


@pytest.mark.parametrize(
    'encoded',
    [
        '{"site":',
        '[]',
        _json_without_root_field(snapshot_to_json(_durable_snapshot()), 'site'),
        _json_with_root_field(snapshot_to_json(_durable_snapshot()), 'unknown', None),
        _json_with_root_field(snapshot_to_json(_durable_snapshot()), 'site', 1),
        _json_with_root_field(
            snapshot_to_json(_durable_snapshot()),
            'revision',
            value=True,
        ),
        _json_with_root_field(snapshot_to_json(_durable_snapshot()), 'parked', 1),
        _json_with_root_field(
            snapshot_to_json(_durable_snapshot()),
            'reader_clock_checkpoint',
            100,
        ),
        _json_with_root_field(
            snapshot_to_json(_durable_snapshot()),
            'strict_capacity_abort_streak',
            value=True,
        ),
        _json_with_root_field(
            snapshot_to_json(_durable_snapshot()),
            'strict_capacity_attempt_token',
            1,
        ),
        _json_with_root_field(snapshot_to_json(_durable_snapshot()), 'status', 'unknown'),
        _json_with_root_field(snapshot_to_json(_durable_snapshot()), 'ordering', 'relaxed'),
        _json_with_root_field(
            snapshot_to_json(_durable_snapshot()),
            'conditions',
            'callback-failed',
        ),
        _json_with_root_field(snapshot_to_json(_durable_snapshot()), 'conditions', [1]),
        _json_with_root_field(snapshot_to_json(_durable_snapshot()), 'manifest', {}),
        _json_with_root_field(snapshot_to_json(_durable_snapshot()), 'callback_marks', {}),
        _json_with_list_entry(
            snapshot_to_json(_durable_snapshot()),
            'callback_marks',
            0,
            [0, 'publish', 'publish-event', 'failed'],
        ),
        _json_with_list_entry(
            snapshot_to_json(_durable_snapshot()),
            'callback_marks',
            0,
            [True, 'publish', 'publish-event', 'failed', 1],
        ),
        _json_with_root_field(snapshot_to_json(_durable_snapshot()), 'epochs', {}),
        _json_with_list_entry(snapshot_to_json(_durable_snapshot()), 'epochs', 0, []),
        _json_without_nested_field(snapshot_to_json(_durable_snapshot()), 'epochs', 'low_id'),
        _json_with_nested_field(
            snapshot_to_json(_durable_snapshot()),
            'epochs',
            'unknown',
            None,
        ),
        _json_with_nested_field(
            snapshot_to_json(_durable_snapshot()),
            'epochs',
            'state',
            'complete',
        ),
        _json_with_nested_field(
            snapshot_to_json(_durable_snapshot()),
            'epochs',
            'epoch_id',
            value=True,
        ),
        _json_with_nested_field(
            snapshot_to_json(_durable_snapshot()),
            'epochs',
            'captured_at',
            100,
        ),
    ],
)
def test_snapshot_from_json_rejects_durable_json_corruption(encoded: str) -> None:
    with pytest.raises(ValueError, match='invalid group snapshot JSON') as error:
        snapshot_from_json(encoded)

    assert error.value.__cause__ is not None


@pytest.mark.parametrize(
    'encoded',
    [
        _json_with_number_literal(
            snapshot_to_json(_durable_snapshot()),
            '"reader_clock_checkpoint":100.5',
            '"reader_clock_checkpoint":NaN',
        ),
        _json_with_number_literal(
            snapshot_to_json(_durable_snapshot()),
            '"reader_clock_checkpoint":100.5',
            '"reader_clock_checkpoint":Infinity',
        ),
        _json_with_number_literal(
            snapshot_to_json(_durable_snapshot()),
            '"reader_clock_checkpoint":100.5',
            '"reader_clock_checkpoint":1e400',
        ),
        _json_with_number_literal(
            snapshot_to_json(_durable_snapshot()),
            '"captured_at":100.0',
            '"captured_at":NaN',
        ),
        _json_with_number_literal(
            snapshot_to_json(_durable_snapshot()),
            '"eligibility_point":400.0',
            '"eligibility_point":Infinity',
        ),
        _json_with_number_literal(
            snapshot_to_json(_durable_snapshot()),
            '"reader_clock_checkpoint":100.0',
            '"reader_clock_checkpoint":1e400',
        ),
    ],
)
def test_snapshot_from_json_rejects_nonfinite_durable_json_floats(encoded: str) -> None:
    with pytest.raises(ValueError, match='invalid group snapshot JSON') as error:
        snapshot_from_json(encoded)

    assert error.value.__cause__ is not None


@pytest.mark.parametrize(
    'omitted',
    [
        ('strict_capacity_abort_streak',),
        ('strict_capacity_attempt_token',),
        ('strict_capacity_abort_streak', 'strict_capacity_attempt_token'),
    ],
)
def test_snapshot_without_strict_capacity_fields_is_rejected(
    omitted: tuple[str, ...],
) -> None:
    payload = cast('dict[str, object]', json.loads(snapshot_to_json(_durable_snapshot())))
    for field in omitted:
        payload.pop(field)

    with pytest.raises(ValueError, match='invalid group snapshot JSON'):
        snapshot_from_json(json.dumps(payload))
