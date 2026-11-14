from __future__ import annotations

import json
import os
from contextlib import ExitStack
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, ClassVar, cast

import pytest

import tests.live._site as site_module
from sg_event_daemon._events import EVENT_FIELDS
from sg_event_daemon._providers import Provider, ProviderCapabilities, ProviderError
from tests._live_mode import LiveOptions
from tests._live_mode import live_options as configured_live_options
from tests.live._daemon import SanitizedLiveError
from tests.live._site import (
    Credential,
    LiveSettings,
    LiveSite,
    load_live_settings,
    open_provider_client,
    validate_live_provider,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from tests.live._site import LiveClient

_COMPLETE_ENVIRONMENT = {
    'SG_EVENT_TEST_CLEANUP_KEY': 'cleanup-key',
    'SG_EVENT_TEST_CLEANUP_SCRIPT': 'quiet-cleanup',
    'SG_EVENT_TEST_PRODUCER_KEY': 'producer-key',
    'SG_EVENT_TEST_PRODUCER_SCRIPT': 'producer',
    'SG_EVENT_TEST_SOURCE_KEY': 'source-key',
    'SG_EVENT_TEST_SOURCE_SCRIPT': 'reader',
    'SG_TEST_PROJECT': 'Live Acceptance',
    'SG_TEST_SITE': 'https://example.invalid',
}
_PROJECT = {'id': 7, 'name': 'Live Acceptance', 'type': 'Project'}
_REQUIRED_LIVE_ENVIRONMENT = frozenset(
    {
        'SG_EVENT_TEST_PRODUCER_KEY',
        'SG_EVENT_TEST_PRODUCER_SCRIPT',
        'SG_EVENT_TEST_SOURCE_KEY',
        'SG_EVENT_TEST_SOURCE_SCRIPT',
        'SG_TEST_PROJECT',
        'SG_TEST_SITE',
    }
)


class _FakeLiveClient:
    def __init__(self, *, operations: list[str] | None = None) -> None:
        self.calls: list[_FindCall] = []
        self.close_error: BaseException | None = None
        self.close_count = 0
        self._create_result: dict[str, object] = {
            'code': 'acceptance-shot',
            'id': 41,
            'project': _PROJECT,
            'type': 'Shot',
        }
        self.created: list[tuple[str, dict[str, object]]] = []
        self.delete_error: BaseException | None = None
        self.deleted: list[tuple[str, int]] = []
        self.find_one_calls: list[tuple[str, object, list[str] | None]] = []
        self._operations = [] if operations is None else operations
        self.pages: list[list[dict[str, object]]] = []
        self.project: dict[str, object] | None = dict(_PROJECT)
        self.residual: dict[str, object] | None = None
        self.serialize_create_payload = False
        self.updated: list[tuple[str, int, dict[str, object]]] = []

    def close(self) -> None:
        self._operations.append('close')
        self.close_count += 1
        if self.close_error is not None:
            raise self.close_error

    def create(self, entity_type: str, data: dict[str, object]) -> dict[str, object]:
        self._operations.append('create')
        if self.serialize_create_payload:
            json.dumps(data)
        self.created.append((entity_type, data))
        return dict(self._create_result)

    def delete(self, entity_type: str, entity_id: int) -> bool:
        self._operations.append('delete')
        self.deleted.append((entity_type, entity_id))
        if self.delete_error is not None:
            raise self.delete_error
        return True

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **kwargs: object,
    ) -> list[dict[str, object]]:
        self.calls.append(_FindCall(entity_type, filters, fields, kwargs))
        if not self.pages:
            return []
        return self.pages.pop(0)

    def find_one(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
    ) -> dict[str, object] | None:
        self._operations.append(f'find-one-{entity_type}')
        self.find_one_calls.append((entity_type, filters, fields))
        if entity_type == 'Project':
            return None if self.project is None else dict(self.project)
        if entity_type == 'Shot':
            return None if self.residual is None else dict(self.residual)
        raise AssertionError(f'unexpected entity type: {entity_type}')

    def update(
        self,
        entity_type: str,
        entity_id: int,
        data: dict[str, object],
    ) -> dict[str, object]:
        self._operations.append('update')
        self.updated.append((entity_type, entity_id, data))
        return {'id': entity_id, 'type': entity_type, **data}


@dataclass(frozen=True)
class _FindCall:
    entity_type: str
    filters: object
    fields: list[str] | None
    kwargs: Mapping[str, object]

    @property
    def page(self) -> object:
        return self.kwargs.get('page')


@dataclass(frozen=True)
class _NamedProviderStub:
    name: str
    capabilities: ProviderCapabilities | None = None


class _FakeShotgun(_FakeLiveClient):
    construction_calls: ClassVar[list[tuple[str, str, bool]]] = []

    def __init__(
        self,
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> None:
        super().__init__()
        self.config = SimpleNamespace(timeout_secs=None)
        type(self).construction_calls.append((site, script_name, connect))
        assert api_key == 'hidden-key'


def _event(event_id: int, phase_value: str | None = None) -> dict[str, object]:
    metadata: dict[str, object] = {'attribute_name': 'description'}
    if phase_value is not None:
        metadata['new_value'] = phase_value
    return {
        'attribute_name': 'description',
        'created_at': datetime(2026, 8, 10, tzinfo=UTC),
        'entity': {'id': 41, 'name': 'acceptance-shot', 'type': 'Shot'},
        'event_type': 'Shotgun_Shot_Change',
        'id': event_id,
        'meta': metadata,
        'project': {'id': 7, 'name': 'Live Acceptance', 'type': 'Project'},
        'session_uuid': None,
        'type': 'EventLogEntry',
        'user': None,
    }


@pytest.fixture
def cleanup_operations() -> list[str]:
    return []


@pytest.fixture
def client() -> _FakeLiveClient:
    return _FakeLiveClient()


@pytest.fixture
def live_options(tmp_path: Path) -> LiveOptions:
    return LiveOptions(
        provider='shotgun-api3',
        mode='sync',
        load_events=1_000,
        report_dir=tmp_path,
        run_live=True,
        run_live_load=False,
    )


@pytest.fixture
def failing_site(live_options: LiveOptions, cleanup_operations: list[str]) -> LiveSite:
    source = _FakeLiveClient(operations=cleanup_operations)
    producer = _FakeLiveClient(operations=cleanup_operations)
    cleanup = _FakeLiveClient(operations=cleanup_operations)
    source.close_error = RuntimeError('source close failed')
    producer.close_error = RuntimeError('producer close failed')
    cleanup.close_error = RuntimeError('cleanup close failed')
    cleanup.delete_error = RuntimeError('delete failed')
    producer.residual = {'code': 'acceptance-shot', 'id': 41, 'type': 'Shot'}
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)
    result = LiveSite(settings, source, producer, cleanup)
    result.create_shot()
    cleanup_operations.clear()
    return result


@pytest.fixture
def site(client: _FakeLiveClient, live_options: LiveOptions) -> LiveSite:
    producer = _FakeLiveClient()
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)
    result = LiveSite(settings, client, producer)
    assert result.create_shot() == 41
    return result


@pytest.fixture
def site_with_quiet_cleanup(
    live_options: LiveOptions,
) -> tuple[LiveSite, _FakeLiveClient, _FakeLiveClient, _FakeLiveClient]:
    source = _FakeLiveClient()
    producer = _FakeLiveClient()
    quiet = _FakeLiveClient()
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)
    return LiveSite(settings, source, producer, quiet), source, producer, quiet


@pytest.fixture
def site_without_cleanup(
    live_options: LiveOptions,
) -> tuple[LiveSite, _FakeLiveClient]:
    source = _FakeLiveClient()
    producer = _FakeLiveClient()
    environment = {
        name: value
        for name, value in _COMPLETE_ENVIRONMENT.items()
        if not name.startswith('SG_EVENT_TEST_CLEANUP_')
    }
    settings = load_live_settings(live_options, environment)
    return LiveSite(settings, source, producer), producer


def test_absent_phase_evidence_uses_exact_count_and_sorted_ids(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    client.pages = [[_event(103), _event(101), _event(102)]]

    events = site.wait_for_events(
        after_id=100,
        expected_values=('one', 'two', 'three'),
        timeout_secs=1,
    )

    assert tuple(event.id for event in events) == (101, 102, 103)
    assert all(event.phase_value is None for event in events)


@pytest.mark.parametrize(
    'missing',
    ['SG_EVENT_TEST_CLEANUP_KEY', 'SG_EVENT_TEST_CLEANUP_SCRIPT'],
)
def test_cleanup_credentials_are_all_or_none(
    live_options: LiveOptions,
    missing: str,
) -> None:
    environment = dict(_COMPLETE_ENVIRONMENT)
    environment.pop(missing)
    with pytest.raises(pytest.UsageError, match=r'cleanup.*both'):
        load_live_settings(live_options, environment)


@pytest.mark.parametrize(
    'empty',
    ['SG_EVENT_TEST_CLEANUP_KEY', 'SG_EVENT_TEST_CLEANUP_SCRIPT'],
)
def test_cleanup_credentials_reject_one_empty_value(
    live_options: LiveOptions,
    empty: str,
) -> None:
    environment = dict(_COMPLETE_ENVIRONMENT)
    environment[empty] = ''
    with pytest.raises(pytest.UsageError, match=r'cleanup.*both'):
        load_live_settings(live_options, environment)


def test_cleanup_credentials_treat_both_empty_values_as_absent(
    live_options: LiveOptions,
) -> None:
    environment = dict(
        _COMPLETE_ENVIRONMENT,
        SG_EVENT_TEST_CLEANUP_KEY='',
        SG_EVENT_TEST_CLEANUP_SCRIPT='',
    )

    settings = load_live_settings(live_options, environment)
    source = _FakeLiveClient()
    producer = _FakeLiveClient()
    site = LiveSite(settings, source, producer)
    site.create_shot()

    assert settings.cleanup is None
    assert site.cleanup() == ()
    assert producer.deleted == [('Shot', 41)]


def test_cleanup_deletes_before_verification_and_every_close(
    failing_site: LiveSite,
    cleanup_operations: list[str],
) -> None:
    site = failing_site
    site.cleanup()

    assert cleanup_operations == [
        'delete',
        'find-one-Shot',
        'close',
        'close',
        'close',
    ]


def test_cleanup_deletes_committed_batches_after_producer_failure() -> None:

    class Client:
        def __init__(self) -> None:
            self.rows: dict[int, dict[str, object]] = {}
            self._calls = 0

        def batch(self, requests: list[dict[str, object]]) -> list[dict[str, object]]:
            self._calls += 1
            result: list[dict[str, object]] = []
            for request in requests:
                if request['request_type'] == 'delete':
                    self.rows.pop(cast('int', request['entity_id']))
                else:
                    identifier = len(self.rows) + 1
                    row = {'id': identifier, **cast('dict[str, object]', request['data'])}
                    self.rows[identifier] = row
                    result.append(row)
            if self._calls == 3:
                raise RuntimeError('response lost after commit')
            return result

        def find_one(self, *_args: object) -> dict[str, object]:
            return {'id': 7, 'type': 'Project', 'name': 'Test'}

        def find(self, *_args: object, **_kwargs: object) -> list[dict[str, object]]:
            return list(self.rows.values())

        def close(self) -> None:
            pass

    client = Client()
    credential = Credential('script', 'key')
    settings = LiveSettings(
        'https://example.invalid',
        'Test',
        credential,
        credential,
        None,
        LiveOptions('shotgun-api3', 'sync', 2, None, run_live=False, run_live_load=False),
    )
    site = LiveSite(settings, cast('LiveClient', client), cast('LiveClient', client))
    site.create_shot_batch(2)
    site.create_shot_batch(2)
    with pytest.raises(RuntimeError, match='response lost'):
        site.create_shot_batch(2)
    assert len(site.bulk_shot_ids) == 4
    assert len(client.rows) == 6
    assert site.cleanup() == ()
    assert client.rows == {}


def test_cleanup_falls_back_to_producer_without_quiet_credentials(
    site_without_cleanup: tuple[LiveSite, _FakeLiveClient],
) -> None:
    site, producer_client = site_without_cleanup
    site.create_shot()
    assert site.cleanup() == ()
    assert producer_client.deleted == [('Shot', site.shot_id)]


def test_cleanup_is_idempotent(
    site_with_quiet_cleanup: tuple[
        LiveSite,
        _FakeLiveClient,
        _FakeLiveClient,
        _FakeLiveClient,
    ],
) -> None:
    site, source, producer, quiet = site_with_quiet_cleanup
    site.create_shot()

    first = site.cleanup()
    second = site.cleanup()

    assert first == second == ()
    assert quiet.deleted == [('Shot', 41)]
    assert source.close_count == 1
    assert producer.close_count == 1
    assert quiet.close_count == 1


def test_cleanup_prefers_complete_quiet_credentials(
    site_with_quiet_cleanup: tuple[
        LiveSite,
        _FakeLiveClient,
        _FakeLiveClient,
        _FakeLiveClient,
    ],
) -> None:
    site, _source_client, producer_client, quiet_client = site_with_quiet_cleanup
    site.create_shot()
    failures = site.cleanup()
    assert failures == ()
    assert quiet_client.deleted == [('Shot', site.shot_id)]
    assert producer_client.deleted == []


def test_cleanup_retains_delete_residual_and_close_failures(failing_site: LiveSite) -> None:
    site = failing_site
    failures = site.cleanup()
    assert [str(error) for error in failures] == [
        'delete failed',
        'residual Shot:41',
        'source close failed',
        'producer close failed',
        'cleanup close failed',
    ]


def test_count_only_correlation_rejects_more_rows_than_requested(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    client.pages = [[_event(101), _event(102)]]

    with pytest.raises(RuntimeError, match=r'expected=1.*observed=2'):
        site.wait_for_events(after_id=100, expected_values=('one',), timeout_secs=1)


@pytest.mark.parametrize('expected_values', [(), ('duplicate', 'duplicate')])
def test_event_discovery_rejects_empty_or_duplicate_expectations_before_query(
    site: LiveSite,
    client: _FakeLiveClient,
    expected_values: tuple[str, ...],
) -> None:
    with pytest.raises(ValueError, match='expected_values'):
        site.wait_for_events(
            after_id=100,
            expected_values=expected_values,
            timeout_secs=1,
        )
    assert client.calls == []


def test_event_query_uses_exact_filters_fields_and_paging(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    phase = f'{site.namespace}-phase'
    client.pages = [[_event(101, phase)]]

    assert [
        event.id
        for event in site.wait_for_events(
            after_id=100,
            expected_values=(phase,),
            timeout_secs=1,
        )
    ] == [101]
    assert client.calls == [
        _FindCall(
            'EventLogEntry',
            [
                ['entity', 'is', {'id': 41, 'type': 'Shot'}],
                ['event_type', 'is', 'Shotgun_Shot_Change'],
                ['attribute_name', 'is', 'description'],
                ['id', 'greater_than', 100],
            ],
            list(EVENT_FIELDS),
            {
                'limit': 500,
                'order': [{'direction': 'asc', 'field_name': 'id'}],
                'page': 1,
            },
        )
    ]


@pytest.mark.parametrize('invalid_id', [True, '101', None])
def test_event_rows_are_validated_before_ids_are_trusted(
    site: LiveSite,
    client: _FakeLiveClient,
    invalid_id: object,
) -> None:
    row = _event(101, f'{site.namespace}-phase')
    row['id'] = invalid_id
    client.pages = [[row]]

    with pytest.raises(TypeError, match='event id'):
        site.wait_for_events(
            after_id=100,
            expected_values=(f'{site.namespace}-phase',),
            timeout_secs=1,
        )


def test_event_rows_require_complete_event_values(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    row = _event(101, f'{site.namespace}-phase')
    row.pop('created_at')
    client.pages = [[row]]

    with pytest.raises(ValueError, match='missing required fields'):
        site.wait_for_events(
            after_id=100,
            expected_values=(f'{site.namespace}-phase',),
            timeout_secs=1,
        )


def test_event_scan_crosses_an_exactly_full_page(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    phases = tuple(f'{site.namespace}-page-{index:03d}' for index in range(501))
    client.pages = [
        [_event(101 + index, phases[index]) for index in range(500)],
        [_event(601, phases[500])],
    ]

    events = site.wait_for_events(
        after_id=100,
        expected_values=phases,
        timeout_secs=1,
    )

    assert tuple(event.id for event in events) == tuple(range(101, 602))
    assert tuple(call.page for call in client.calls) == (1, 2)
    assert all(call.fields == list(EVENT_FIELDS) for call in client.calls)


def test_exact_event_filter_excludes_other_rows(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    phase = f'{site.namespace}-exact'
    other_shot = _event(102, phase)
    other_shot['entity'] = {'id': 42, 'name': 'other', 'type': 'Shot'}
    other_event = _event(103, phase)
    other_event['event_type'] = 'Shotgun_Shot_New'
    other_attribute = _event(104, phase)
    other_attribute['attribute_name'] = 'code'
    client.pages = [[_event(101, phase), other_shot, other_event, other_attribute]]

    events = site.wait_for_events(after_id=100, expected_values=(phase,), timeout_secs=1)

    assert tuple(event.id for event in events) == (101,)


def test_exposed_phase_evidence_matches_requested_values_exactly(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    phase = f'{site.namespace}-requested'
    client.pages = [[_event(101, 'unrelated'), _event(102, phase)]]

    events = site.wait_for_events(after_id=100, expected_values=(phase,), timeout_secs=1)

    assert tuple(event.id for event in events) == (102,)
    assert tuple(event.phase_value for event in events) == (phase,)


def test_full_scan_detects_duplicate_evidence_on_following_page(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    phases = tuple(f'{site.namespace}-page-{index:03d}' for index in range(500))
    client.pages = [
        [_event(101 + index, phases[index]) for index in range(500)],
        [_event(601, phases[-1])],
    ]

    with pytest.raises(RuntimeError, match='duplicate phase evidence'):
        site.wait_for_events(after_id=100, expected_values=phases, timeout_secs=1)

    assert tuple(call.page for call in client.calls) == (1, 2)


@pytest.mark.parametrize(
    'project',
    [None, {'id': 7, 'name': 'Other Project', 'type': 'Project'}],
)
def test_live_site_requires_exact_project_match(
    live_options: LiveOptions,
    project: dict[str, object] | None,
) -> None:
    producer = _FakeLiveClient()
    producer.project = project
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)

    with pytest.raises(pytest.UsageError, match=r'Project.*Live Acceptance'):
        LiveSite(settings, _FakeLiveClient(), producer)


def test_live_site_resolves_project_exactly_before_creating_shot(
    live_options: LiveOptions,
) -> None:
    source = _FakeLiveClient()
    producer = _FakeLiveClient()
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)

    site = LiveSite(settings, source, producer)
    shot_id = site.create_shot()

    assert producer.find_one_calls == [
        ('Project', [['name', 'is', 'Live Acceptance']], ['id', 'name', 'type'])
    ]
    assert shot_id == 41
    assert site.shot_id == 41
    assert producer.created == [('Shot', {'code': f'{site.namespace}-shot', 'project': _PROJECT})]


def test_live_site_sends_json_serializable_project_to_provider(
    live_options: LiveOptions,
) -> None:
    producer = _FakeLiveClient()
    producer.serialize_create_payload = True
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)
    site = LiveSite(settings, _FakeLiveClient(), producer)

    assert site.create_shot() == 41


def test_mixed_exposed_and_absent_phase_evidence_is_ambiguous(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    phase = f'{site.namespace}-requested'
    client.pages = [[_event(101, phase), _event(102)]]

    with pytest.raises(RuntimeError, match=r'ambiguous.*phase evidence'):
        site.wait_for_events(after_id=100, expected_values=(phase,), timeout_secs=1)


def test_non_string_phase_evidence_uses_count_only_fallback(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    row = _event(101)
    row['meta'] = {'attribute_name': 'description', 'new_value': 7}
    client.pages = [[row]]

    events = site.wait_for_events(
        after_id=100,
        expected_values=('one',),
        timeout_secs=1,
    )

    assert tuple(event.id for event in events) == (101,)
    assert events[0].phase_value is None


def test_open_provider_client_imports_public_sync_client_lazily(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    imports: list[str] = []

    def _fake_import(name: str) -> object:
        imports.append(name)
        return SimpleNamespace(Shotgun=_FakeShotgun)

    monkeypatch.setattr(site_module, 'import_module', _fake_import)
    provider = _NamedProviderStub(
        capabilities=ProviderCapabilities(sync=True, async_=False),
        name='shotgun-api3',
    )
    _FakeShotgun.construction_calls.clear()

    result = open_provider_client(
        provider,
        'https://example.invalid',
        Credential('writer', 'hidden-key'),
    )

    assert isinstance(result, _FakeShotgun)
    assert imports == ['shotgun_api3']
    assert _FakeShotgun.construction_calls == [('https://example.invalid', 'writer', False)]
    assert result.config.timeout_secs == 30.0


def test_open_provider_client_rejects_missing_public_sync_export(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_import(_name: str) -> object:
        return SimpleNamespace(Shotgun=None)

    monkeypatch.setattr(site_module, 'import_module', _fake_import)
    provider = _NamedProviderStub(name='shotgun-api3')

    with pytest.raises(ProviderError, match='Shotgun'):
        open_provider_client(
            provider,
            'https://example.invalid',
            Credential('writer', 'hidden-key'),
        )


def test_partial_scan_progress_is_retained_on_timeout(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    present = f'{site.namespace}-present'
    client.pages = [[_event(101, present)]]

    with pytest.raises(RuntimeError, match='observed=1'):
        site.wait_for_events(
            after_id=100,
            expected_values=(present, f'{site.namespace}-missing'),
            timeout_secs=0,
        )

    assert tuple(event.id for event in site.observed_events) == (101,)


def test_repeated_scan_discovers_a_late_lower_id(
    site: LiveSite,
    client: _FakeLiveClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    phases = tuple(f'{site.namespace}-late-{index}' for index in range(3))
    client.pages = [
        [_event(102, phases[1]), _event(103, phases[2])],
        [_event(101, phases[0]), _event(102, phases[1]), _event(103, phases[2])],
    ]
    waits: list[float] = []
    monkeypatch.setattr(
        site_module,
        'time',
        SimpleNamespace(monotonic=lambda: 0.0, sleep=waits.append),
    )

    events = site.wait_for_events(
        after_id=100,
        expected_values=phases,
        timeout_secs=1,
    )

    assert tuple(event.id for event in events) == (101, 102, 103)
    assert waits == [0.05]
    assert tuple(call.page for call in client.calls) == (1, 1)
    assert all(
        ['id', 'greater_than', 100] in cast('list[list[object]]', call.filters)
        for call in client.calls
    )


@pytest.mark.parametrize('missing', sorted(_REQUIRED_LIVE_ENVIRONMENT))
def test_selected_settings_reject_each_missing_required_value(
    live_options: LiveOptions,
    missing: str,
) -> None:
    environment = dict(_COMPLETE_ENVIRONMENT)
    environment.pop(missing)
    with pytest.raises(pytest.UsageError, match=missing):
        load_live_settings(live_options, environment)


class _RecoveryClient(_FakeLiveClient):
    def __init__(self) -> None:
        super().__init__()
        self.rows: dict[int, dict[str, object]] = {}
        self.batches: list[list[dict[str, object]]] = []
        self.retain: set[int] = set()

    def batch(self, requests: list[dict[str, object]]) -> list[dict[str, object]]:
        self.batches.append(requests)
        results: list[dict[str, object]] = []
        for request in requests:
            assert request['entity_type'] == 'Shot'
            if request['request_type'] == 'create':
                data = cast('dict[str, object]', request['data'])
                identifier = len(self.rows) + 100
                self.rows[identifier] = dict(data, id=identifier, type='Shot')
                results.append(self.rows[identifier])
            else:
                assert request['request_type'] == 'delete'
                identifier = cast('int', request['entity_id'])
                if identifier not in self.retain:
                    del self.rows[identifier]
        return results

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **kwargs: object,
    ) -> list[dict[str, object]]:
        assert entity_type == 'Shot'
        assert fields == ['id']
        del kwargs
        clauses = cast('list[object]', filters)
        project_filter = cast('list[object]', clauses[0])
        assert project_filter[:2] == ['project', 'is']
        any_filter = cast('dict[str, object]', clauses[1])
        assert any_filter['filter_operator'] == 'any'
        code_filters = cast('list[list[str]]', any_filter['filters'])
        assert code_filters[0][:2] == ['code', 'is']
        assert code_filters[1][:2] == ['code', 'starts_with']
        return [
            {'id': identifier}
            for identifier, row in self.rows.items()
            if row['project'] == project_filter[2]
            and (
                row['code'] == code_filters[0][2]
                or str(row['code']).startswith(code_filters[1][2])
            )
        ]


def test_given_namespace_names_the_run_shots(live_options: LiveOptions) -> None:
    namespace = 'sg-event-daemon-20261031T120000000000Z-' + 'a' * 32
    options = replace(live_options, namespace=namespace)
    producer = _RecoveryClient()
    site = LiveSite(
        load_live_settings(options, _COMPLETE_ENVIRONMENT), _FakeLiveClient(), producer
    )
    site.create_shot()
    site.create_shot_batch(2)
    assert producer.created[0][1]['code'] == f'{namespace}-shot'
    assert [row['code'] for row in producer.rows.values()] == [
        f'{namespace}-bulk-0',
        f'{namespace}-bulk-1',
    ]


@pytest.mark.parametrize('residual', [False, True])
def test_recovery_deletes_only_the_named_namespace(
    live_options: LiveOptions,
    residual: bool,
) -> None:
    namespace = 'sg-event-daemon-20261031T120000000000Z-' + 'a' * 32
    other = 'sg-event-daemon-20261031T120000000000Z-' + 'b' * 32
    cleanup = _RecoveryClient()
    project = {'type': 'Project', 'id': 7}
    cleanup.rows = {
        identifier: {'code': f'{namespace}-bulk-{identifier}', 'project': project}
        for identifier in range(1, 102)
    }
    cleanup.rows.update(
        {
            102: {'code': f'{namespace}-shot', 'project': project},
            103: {'code': f'{other}-shot', 'project': project},
            104: {'code': f'{other}-bulk-1', 'project': project},
            105: {'code': f'{namespace}-shot-extra', 'project': project},
            106: {'code': f'{namespace}-shot', 'project': {'type': 'Project', 'id': 8}},
        }
    )
    if residual:
        cleanup.retain.add(102)
    source, producer = _FakeLiveClient(), _FakeLiveClient()
    site = LiveSite(
        load_live_settings(live_options, _COMPLETE_ENVIRONMENT), source, producer, cleanup
    )
    assert site.shot_id is None
    failures = site.recover(namespace)
    assert set(cleanup.rows) == ({102, 103, 104, 105, 106} if residual else {103, 104, 105, 106})
    assert [len(batch) for batch in cleanup.batches] == [100, 2]
    assert [str(error) for error in failures] == (
        ['residual Shot: 1 recovery rows'] if residual else []
    )
    assert [client.close_count for client in (source, producer, cleanup)] == [1, 1, 1]


@pytest.mark.live
def test_recover_live_namespace(pytestconfig: pytest.Config) -> None:
    namespace = pytestconfig.getoption('live_recover')
    if namespace is None:
        pytest.skip('requires --live-recover')
    options = configured_live_options(pytestconfig)
    settings = load_live_settings(options, os.environ)
    provider = validate_live_provider(options)
    with ExitStack() as pending:
        source = open_provider_client(provider, settings.site, settings.source)
        pending.callback(source.close)
        producer = open_provider_client(provider, settings.site, settings.producer)
        pending.callback(producer.close)
        cleanup = None
        if settings.cleanup is not None:
            cleanup = open_provider_client(provider, settings.site, settings.cleanup)
            pending.callback(cleanup.close)
        site = LiveSite(settings, source, producer, cleanup)
        # The resolved site owns every client, including recovery failure cleanup.
        pending.pop_all()
    failures = site.recover(namespace)
    if failures:
        secrets = [settings.source.api_key, settings.producer.api_key]
        if settings.cleanup is not None:
            secrets.append(settings.cleanup.api_key)
        raise BaseExceptionGroup(
            'live namespace recovery failed',
            [SanitizedLiveError('cleanup', error, secrets) for error in failures],
        )


@pytest.mark.parametrize('close_failure', [False, True])
def test_recovery_closes_clients_when_namespace_is_empty(
    site_with_quiet_cleanup: tuple[LiveSite, _FakeLiveClient, _FakeLiveClient, _FakeLiveClient],
    close_failure: bool,
) -> None:
    site, source, producer, cleanup = site_with_quiet_cleanup
    failure = RuntimeError('source close failed')
    if close_failure:
        source.close_error = failure
    namespace = 'sg-event-daemon-20261031T120000000000Z-' + 'a' * 32
    assert site.recover(namespace) == ((failure,) if close_failure else ())
    assert [client.close_count for client in (source, producer, cleanup)] == [1, 1, 1]


def test_settings_keep_source_producer_and_cleanup_roles_distinct(
    live_options: LiveOptions,
) -> None:
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)
    assert settings.source.script_name == 'reader'
    assert settings.producer.script_name == 'producer'
    assert settings.cleanup is not None
    assert settings.cleanup.script_name == 'quiet-cleanup'
    assert 'source-key' not in repr(settings)
    assert 'producer-key' not in repr(settings)


def test_settings_require_a_site_hostname(live_options: LiveOptions) -> None:
    environment = dict(_COMPLETE_ENVIRONMENT, SG_TEST_SITE='not-a-site-url')
    with pytest.raises(pytest.UsageError, match=r'SG_TEST_SITE.*hostname'):
        load_live_settings(live_options, environment)


def test_timeout_reports_only_entity_and_counts(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    raw_row = _event(101, 'unrelated-raw-value')
    client.pages = [[raw_row]]

    with pytest.raises(RuntimeError) as captured:
        site.wait_for_events(
            after_id=100,
            expected_values=(f'{site.namespace}-missing',),
            timeout_secs=0,
        )

    message = str(captured.value)
    assert 'Shot:41' in message
    assert 'expected=1' in message
    assert 'observed=0' in message
    assert 'source-key' not in message
    assert 'producer-key' not in message
    assert 'unrelated-raw-value' not in message
    assert repr(raw_row) not in message


def test_update_description_requires_and_uses_tracked_shot(
    live_options: LiveOptions,
) -> None:
    producer = _FakeLiveClient()
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)
    site = LiveSite(settings, _FakeLiveClient(), producer)
    with pytest.raises(RuntimeError, match='Shot'):
        site.update_description('too-early')

    site.create_shot()
    site.update_description('phase-one')

    assert producer.updated == [('Shot', 41, {'description': 'phase-one'})]


def test_validate_live_provider_requests_selected_capabilities(
    monkeypatch: pytest.MonkeyPatch,
    live_options: LiveOptions,
) -> None:
    calls: list[tuple[str, frozenset[str]]] = []
    sentinel = cast('Provider', object())

    def _fake_load_provider(name: str, required: set[str]) -> Provider:
        calls.append((name, frozenset(required)))
        return sentinel

    monkeypatch.setattr(site_module, 'load_provider', _fake_load_provider)

    assert validate_live_provider(live_options) is sentinel
    assert calls == [('shotgun-api3', frozenset({'sync'}))]

    async_options = LiveOptions(
        provider='sg-python-api',
        mode='sync+async',
        load_events=1_000,
        report_dir=None,
        run_live=True,
        run_live_load=False,
    )
    assert validate_live_provider(async_options) is sentinel
    assert calls[-1] == ('sg-python-api', frozenset({'async', 'sync'}))
