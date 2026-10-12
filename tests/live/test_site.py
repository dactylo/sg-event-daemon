from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, ClassVar, cast

import pytest

import tests.live._site as site_module
from sg_event_daemon._events import EVENT_FIELDS
from sg_event_daemon._providers import Provider, ProviderCapabilities, ProviderError
from tests._live_mode import _LiveOptions
from tests.live._site import (
    LiveSite,
    _Credential,
    load_live_settings,
    open_provider_client,
    validate_live_provider,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

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
        self._calls: list[_FindCall] = []
        self._close_error: BaseException | None = None
        self._close_count = 0
        self._create_result: dict[str, object] = {
            'code': 'acceptance-shot',
            'id': 41,
            'project': _PROJECT,
            'type': 'Shot',
        }
        self._created: list[tuple[str, dict[str, object]]] = []
        self._delete_error: BaseException | None = None
        self._deleted: list[tuple[str, int]] = []
        self._find_one_calls: list[tuple[str, object, list[str] | None]] = []
        self._operations = [] if operations is None else operations
        self._pages: list[list[dict[str, object]]] = []
        self._project: dict[str, object] | None = dict(_PROJECT)
        self._residual: dict[str, object] | None = None
        self._serialize_create_payload = False
        self._updated: list[tuple[str, int, dict[str, object]]] = []

    def close(self) -> None:
        self._operations.append('close')
        self._close_count += 1
        if self._close_error is not None:
            raise self._close_error

    def create(self, entity_type: str, data: dict[str, object]) -> dict[str, object]:
        self._operations.append('create')
        if self._serialize_create_payload:
            json.dumps(data)
        self._created.append((entity_type, data))
        return dict(self._create_result)

    def delete(self, entity_type: str, entity_id: int) -> bool:
        self._operations.append('delete')
        self._deleted.append((entity_type, entity_id))
        if self._delete_error is not None:
            raise self._delete_error
        return True

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **kwargs: object,
    ) -> list[dict[str, object]]:
        self._calls.append(_FindCall(entity_type, filters, fields, kwargs))
        if not self._pages:
            return []
        return self._pages.pop(0)

    def find_one(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
    ) -> dict[str, object] | None:
        self._operations.append(f'find-one-{entity_type}')
        self._find_one_calls.append((entity_type, filters, fields))
        if entity_type == 'Project':
            return None if self._project is None else dict(self._project)
        if entity_type == 'Shot':
            return None if self._residual is None else dict(self._residual)
        raise AssertionError(f'unexpected entity type: {entity_type}')

    def update(
        self,
        entity_type: str,
        entity_id: int,
        data: dict[str, object],
    ) -> dict[str, object]:
        self._operations.append('update')
        self._updated.append((entity_type, entity_id, data))
        return {'id': entity_id, 'type': entity_type, **data}


@dataclass(frozen=True)
class _FindCall:
    _entity_type: str
    _filters: object
    _fields: list[str] | None
    _kwargs: Mapping[str, object]

    @property
    def _page(self) -> object:
        return self._kwargs.get('page')


@dataclass(frozen=True)
class _NamedProviderStub:
    name: str
    capabilities: ProviderCapabilities | None = None


class _FakeShotgun(_FakeLiveClient):
    _construction_calls: ClassVar[list[tuple[str, str, bool]]] = []

    def __init__(
        self,
        site: str,
        *,
        script_name: str,
        api_key: str,
        connect: bool,
    ) -> None:
        super().__init__()
        type(self)._construction_calls.append((site, script_name, connect))
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
def live_options(tmp_path: Path) -> _LiveOptions:
    return _LiveOptions(
        _provider='shotgun-api3',
        _mode='sync',
        _load_events=1_000,
        _report_dir=tmp_path,
        _run_live=True,
        _run_live_load=False,
    )


@pytest.fixture
def failing_site(live_options: _LiveOptions, cleanup_operations: list[str]) -> LiveSite:
    source = _FakeLiveClient(operations=cleanup_operations)
    producer = _FakeLiveClient(operations=cleanup_operations)
    cleanup = _FakeLiveClient(operations=cleanup_operations)
    source._close_error = RuntimeError('source close failed')
    producer._close_error = RuntimeError('producer close failed')
    cleanup._close_error = RuntimeError('cleanup close failed')
    cleanup._delete_error = RuntimeError('delete failed')
    producer._residual = {'code': 'acceptance-shot', 'id': 41, 'type': 'Shot'}
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)
    result = LiveSite(settings, source, producer, cleanup)
    result._create_shot()
    cleanup_operations.clear()
    return result


@pytest.fixture
def site(client: _FakeLiveClient, live_options: _LiveOptions) -> LiveSite:
    producer = _FakeLiveClient()
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)
    result = LiveSite(settings, client, producer)
    assert result._create_shot() == 41
    return result


@pytest.fixture
def site_with_quiet_cleanup(
    live_options: _LiveOptions,
) -> tuple[LiveSite, _FakeLiveClient, _FakeLiveClient, _FakeLiveClient]:
    source = _FakeLiveClient()
    producer = _FakeLiveClient()
    quiet = _FakeLiveClient()
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)
    return LiveSite(settings, source, producer, quiet), source, producer, quiet


@pytest.fixture
def site_without_cleanup(
    live_options: _LiveOptions,
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
    client._pages = [[_event(103), _event(101), _event(102)]]

    events = site._wait_for_events(
        after_id=100,
        expected_values=('one', 'two', 'three'),
        timeout_secs=1,
    )

    assert tuple(event._id for event in events) == (101, 102, 103)
    assert all(event._phase_value is None for event in events)


@pytest.mark.parametrize(
    'missing',
    ['SG_EVENT_TEST_CLEANUP_KEY', 'SG_EVENT_TEST_CLEANUP_SCRIPT'],
)
def test_cleanup_credentials_are_all_or_none(
    live_options: _LiveOptions,
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
    live_options: _LiveOptions,
    empty: str,
) -> None:
    environment = dict(_COMPLETE_ENVIRONMENT)
    environment[empty] = ''
    with pytest.raises(pytest.UsageError, match=r'cleanup.*both'):
        load_live_settings(live_options, environment)


def test_cleanup_credentials_treat_both_empty_values_as_absent(
    live_options: _LiveOptions,
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
    site._create_shot()

    assert settings._cleanup is None
    assert site._cleanup() == ()
    assert producer._deleted == [('Shot', 41)]


def test_cleanup_deletes_before_verification_and_every_close(
    failing_site: LiveSite,
    cleanup_operations: list[str],
) -> None:
    site = failing_site
    site._cleanup()

    assert cleanup_operations == [
        'delete',
        'find-one-Shot',
        'close',
        'close',
        'close',
    ]


def test_cleanup_falls_back_to_producer_without_quiet_credentials(
    site_without_cleanup: tuple[LiveSite, _FakeLiveClient],
) -> None:
    site, producer_client = site_without_cleanup
    site._create_shot()
    assert site._cleanup() == ()
    assert producer_client._deleted == [('Shot', site._shot_id)]


def test_cleanup_is_idempotent(
    site_with_quiet_cleanup: tuple[
        LiveSite,
        _FakeLiveClient,
        _FakeLiveClient,
        _FakeLiveClient,
    ],
) -> None:
    site, source, producer, quiet = site_with_quiet_cleanup
    site._create_shot()

    first = site._cleanup()
    second = site._cleanup()

    assert first == second == ()
    assert quiet._deleted == [('Shot', 41)]
    assert source._close_count == 1
    assert producer._close_count == 1
    assert quiet._close_count == 1


def test_cleanup_prefers_complete_quiet_credentials(
    site_with_quiet_cleanup: tuple[
        LiveSite,
        _FakeLiveClient,
        _FakeLiveClient,
        _FakeLiveClient,
    ],
) -> None:
    site, _source_client, producer_client, quiet_client = site_with_quiet_cleanup
    site._create_shot()
    failures = site._cleanup()
    assert failures == ()
    assert quiet_client._deleted == [('Shot', site._shot_id)]
    assert producer_client._deleted == []


def test_cleanup_retains_delete_residual_and_close_failures(failing_site: LiveSite) -> None:
    site = failing_site
    failures = site._cleanup()
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
    client._pages = [[_event(101), _event(102)]]

    with pytest.raises(RuntimeError, match=r'expected=1.*observed=2'):
        site._wait_for_events(after_id=100, expected_values=('one',), timeout_secs=1)


@pytest.mark.parametrize('expected_values', [(), ('duplicate', 'duplicate')])
def test_event_discovery_rejects_empty_or_duplicate_expectations_before_query(
    site: LiveSite,
    client: _FakeLiveClient,
    expected_values: tuple[str, ...],
) -> None:
    with pytest.raises(ValueError, match='expected_values'):
        site._wait_for_events(
            after_id=100,
            expected_values=expected_values,
            timeout_secs=1,
        )
    assert client._calls == []


def test_event_query_uses_exact_filters_fields_and_paging(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    phase = f'{site._namespace}-phase'
    client._pages = [[_event(101, phase)]]

    assert [
        event._id
        for event in site._wait_for_events(
            after_id=100,
            expected_values=(phase,),
            timeout_secs=1,
        )
    ] == [101]
    assert client._calls == [
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
    row = _event(101, f'{site._namespace}-phase')
    row['id'] = invalid_id
    client._pages = [[row]]

    with pytest.raises(TypeError, match='event id'):
        site._wait_for_events(
            after_id=100,
            expected_values=(f'{site._namespace}-phase',),
            timeout_secs=1,
        )


def test_event_rows_require_complete_event_values(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    row = _event(101, f'{site._namespace}-phase')
    row.pop('created_at')
    client._pages = [[row]]

    with pytest.raises(ValueError, match='missing required fields'):
        site._wait_for_events(
            after_id=100,
            expected_values=(f'{site._namespace}-phase',),
            timeout_secs=1,
        )


def test_event_scan_crosses_an_exactly_full_page(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    phases = tuple(f'{site._namespace}-page-{index:03d}' for index in range(501))
    client._pages = [
        [_event(101 + index, phases[index]) for index in range(500)],
        [_event(601, phases[500])],
    ]

    events = site._wait_for_events(
        after_id=100,
        expected_values=phases,
        timeout_secs=1,
    )

    assert tuple(event._id for event in events) == tuple(range(101, 602))
    assert tuple(call._page for call in client._calls) == (1, 2)
    assert all(call._fields == list(EVENT_FIELDS) for call in client._calls)


def test_exact_event_filter_excludes_other_rows(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    phase = f'{site._namespace}-exact'
    other_shot = _event(102, phase)
    other_shot['entity'] = {'id': 42, 'name': 'other', 'type': 'Shot'}
    other_event = _event(103, phase)
    other_event['event_type'] = 'Shotgun_Shot_New'
    other_attribute = _event(104, phase)
    other_attribute['attribute_name'] = 'code'
    client._pages = [[_event(101, phase), other_shot, other_event, other_attribute]]

    events = site._wait_for_events(after_id=100, expected_values=(phase,), timeout_secs=1)

    assert tuple(event._id for event in events) == (101,)


def test_exposed_phase_evidence_matches_requested_values_exactly(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    phase = f'{site._namespace}-requested'
    client._pages = [[_event(101, 'unrelated'), _event(102, phase)]]

    events = site._wait_for_events(after_id=100, expected_values=(phase,), timeout_secs=1)

    assert tuple(event._id for event in events) == (102,)
    assert tuple(event._phase_value for event in events) == (phase,)


def test_full_scan_detects_duplicate_evidence_on_following_page(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    phases = tuple(f'{site._namespace}-page-{index:03d}' for index in range(500))
    client._pages = [
        [_event(101 + index, phases[index]) for index in range(500)],
        [_event(601, phases[-1])],
    ]

    with pytest.raises(RuntimeError, match='duplicate phase evidence'):
        site._wait_for_events(after_id=100, expected_values=phases, timeout_secs=1)

    assert tuple(call._page for call in client._calls) == (1, 2)


@pytest.mark.parametrize(
    'project',
    [None, {'id': 7, 'name': 'Other Project', 'type': 'Project'}],
)
def test_live_site_requires_exact_project_match(
    live_options: _LiveOptions,
    project: dict[str, object] | None,
) -> None:
    producer = _FakeLiveClient()
    producer._project = project
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)

    with pytest.raises(pytest.UsageError, match=r'Project.*Live Acceptance'):
        LiveSite(settings, _FakeLiveClient(), producer)


def test_live_site_resolves_project_exactly_before_creating_shot(
    live_options: _LiveOptions,
) -> None:
    source = _FakeLiveClient()
    producer = _FakeLiveClient()
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)

    site = LiveSite(settings, source, producer)
    shot_id = site._create_shot()

    assert producer._find_one_calls == [
        ('Project', [['name', 'is', 'Live Acceptance']], ['id', 'name', 'type'])
    ]
    assert shot_id == 41
    assert site._shot_id == 41
    assert producer._created == [
        ('Shot', {'code': f'{site._namespace}-shot', 'project': _PROJECT})
    ]


def test_live_site_sends_json_serializable_project_to_provider(
    live_options: _LiveOptions,
) -> None:
    producer = _FakeLiveClient()
    producer._serialize_create_payload = True
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)
    site = LiveSite(settings, _FakeLiveClient(), producer)

    assert site._create_shot() == 41


def test_mixed_exposed_and_absent_phase_evidence_is_ambiguous(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    phase = f'{site._namespace}-requested'
    client._pages = [[_event(101, phase), _event(102)]]

    with pytest.raises(RuntimeError, match=r'ambiguous.*phase evidence'):
        site._wait_for_events(after_id=100, expected_values=(phase,), timeout_secs=1)


def test_non_string_phase_evidence_uses_count_only_fallback(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    row = _event(101)
    row['meta'] = {'attribute_name': 'description', 'new_value': 7}
    client._pages = [[row]]

    events = site._wait_for_events(
        after_id=100,
        expected_values=('one',),
        timeout_secs=1,
    )

    assert tuple(event._id for event in events) == (101,)
    assert events[0]._phase_value is None


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
    _FakeShotgun._construction_calls.clear()

    result = open_provider_client(
        provider,
        'https://example.invalid',
        _Credential('writer', 'hidden-key'),
    )

    assert isinstance(result, _FakeLiveClient)
    assert imports == ['shotgun_api3']
    assert _FakeShotgun._construction_calls == [('https://example.invalid', 'writer', False)]


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
            _Credential('writer', 'hidden-key'),
        )


def test_partial_scan_progress_is_retained_on_timeout(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    present = f'{site._namespace}-present'
    client._pages = [[_event(101, present)]]

    with pytest.raises(RuntimeError, match='observed=1'):
        site._wait_for_events(
            after_id=100,
            expected_values=(present, f'{site._namespace}-missing'),
            timeout_secs=0,
        )

    assert tuple(event._id for event in site._observed_events) == (101,)


def test_repeated_scan_discovers_a_late_lower_id(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    phases = tuple(f'{site._namespace}-late-{index}' for index in range(3))
    client._pages = [
        [_event(102, phases[1]), _event(103, phases[2])],
        [_event(101, phases[0]), _event(102, phases[1]), _event(103, phases[2])],
    ]

    events = site._wait_for_events(
        after_id=100,
        expected_values=phases,
        timeout_secs=1,
    )

    assert tuple(event._id for event in events) == (101, 102, 103)
    assert tuple(call._page for call in client._calls) == (1, 1)
    assert all(
        ['id', 'greater_than', 100] in cast('list[list[object]]', call._filters)
        for call in client._calls
    )


@pytest.mark.parametrize('missing', sorted(_REQUIRED_LIVE_ENVIRONMENT))
def test_selected_settings_reject_each_missing_required_value(
    live_options: _LiveOptions,
    missing: str,
) -> None:
    environment = dict(_COMPLETE_ENVIRONMENT)
    environment.pop(missing)
    with pytest.raises(pytest.UsageError, match=missing):
        load_live_settings(live_options, environment)


def test_settings_keep_source_producer_and_cleanup_roles_distinct(
    live_options: _LiveOptions,
) -> None:
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)
    assert settings._source._script_name == 'reader'
    assert settings._producer._script_name == 'producer'
    assert settings._cleanup is not None
    assert settings._cleanup._script_name == 'quiet-cleanup'
    assert 'source-key' not in repr(settings)
    assert 'producer-key' not in repr(settings)


def test_settings_require_a_site_hostname(live_options: _LiveOptions) -> None:
    environment = dict(_COMPLETE_ENVIRONMENT, SG_TEST_SITE='not-a-site-url')
    with pytest.raises(pytest.UsageError, match=r'SG_TEST_SITE.*hostname'):
        load_live_settings(live_options, environment)


def test_timeout_reports_only_entity_and_counts(
    site: LiveSite,
    client: _FakeLiveClient,
) -> None:
    raw_row = _event(101, 'unrelated-raw-value')
    client._pages = [[raw_row]]

    with pytest.raises(RuntimeError) as captured:
        site._wait_for_events(
            after_id=100,
            expected_values=(f'{site._namespace}-missing',),
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
    live_options: _LiveOptions,
) -> None:
    producer = _FakeLiveClient()
    settings = load_live_settings(live_options, _COMPLETE_ENVIRONMENT)
    site = LiveSite(settings, _FakeLiveClient(), producer)
    with pytest.raises(RuntimeError, match='Shot'):
        site._update_description('too-early')

    site._create_shot()
    site._update_description('phase-one')

    assert producer._updated == [('Shot', 41, {'description': 'phase-one'})]


def test_validate_live_provider_requests_selected_capabilities(
    monkeypatch: pytest.MonkeyPatch,
    live_options: _LiveOptions,
) -> None:
    calls: list[tuple[str, frozenset[str]]] = []
    sentinel = cast('Provider', object())

    def _fake_load_provider(name: str, required: set[str]) -> Provider:
        calls.append((name, frozenset(required)))
        return sentinel

    monkeypatch.setattr(site_module, 'load_provider', _fake_load_provider)

    assert validate_live_provider(live_options) is sentinel
    assert calls == [('shotgun-api3', frozenset({'sync'}))]

    async_options = _LiveOptions(
        _provider='sg-python-api',
        _mode='sync+async',
        _load_events=1_000,
        _report_dir=None,
        _run_live=True,
        _run_live_load=False,
    )
    assert validate_live_provider(async_options) is sentinel
    assert calls[-1] == ('sg-python-api', frozenset({'async', 'sync'}))
