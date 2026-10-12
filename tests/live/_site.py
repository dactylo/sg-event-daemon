from __future__ import annotations

import time
import uuid
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib import import_module
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol, cast
from urllib.parse import urlsplit

import pytest

from sg_event_daemon._events import EVENT_FIELDS, Event
from sg_event_daemon._providers import Provider, ProviderError, load_provider

if TYPE_CHECKING:
    from tests._live_mode import _LiveOptions

_EVENT_FILTER_FIELDS = list(EVENT_FIELDS)
_EVENT_PAGE_LIMIT = 500
_PROJECT_FIELDS = ['id', 'name', 'type']
_SHOT_FIELDS = ['id', 'code', 'type']


@dataclass(frozen=True)
class _Credential:
    _script_name: str
    _api_key: str = field(repr=False)


class _LiveClient(Protocol):
    def close(self) -> object: ...

    def create(self, entity_type: str, data: dict[str, object]) -> dict[str, object]: ...

    def delete(self, entity_type: str, entity_id: int) -> bool: ...

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **kwargs: object,
    ) -> list[dict[str, object]]: ...

    def find_one(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
    ) -> dict[str, object] | None: ...

    def update(
        self,
        entity_type: str,
        entity_id: int,
        data: dict[str, object],
    ) -> dict[str, object]: ...


class _NamedProvider(Protocol):
    @property
    def name(self) -> str: ...


@dataclass(frozen=True)
class _LiveEvent:
    _id: int
    _phase_value: str | None
    _value: Event


@dataclass(frozen=True)
class _LiveSettings:
    _site: str
    _project: str
    _source: _Credential
    _producer: _Credential
    _cleanup: _Credential | None
    _options: _LiveOptions


class LiveSite:
    _namespace: str
    _observed_events: tuple[_LiveEvent, ...]
    _project: Mapping[str, object]
    _shot_id: int | None

    def __init__(
        self,
        settings: _LiveSettings,
        source_client: _LiveClient,
        producer_client: _LiveClient,
        cleanup_client: _LiveClient | None = None,
    ) -> None:
        self._cleanup_client = cleanup_client
        self._cleanup_failures: tuple[BaseException, ...] | None = None
        self._producer_client = producer_client
        self._settings = settings
        self._source_client = source_client
        self._namespace = _namespace()
        self._observed_events = ()
        self._shot_id = None
        project = producer_client.find_one(
            'Project',
            [['name', 'is', settings._project]],
            list(_PROJECT_FIELDS),
        )
        if not self._valid_project(project, settings._project):
            raise pytest.UsageError(f'Project {settings._project!r} was not found exactly')
        project_mapping = cast('dict[str, object]', project)
        self._project = MappingProxyType(dict(project_mapping))

    def _accepted_events(
        self,
        candidates: Mapping[int, _LiveEvent],
        expected: frozenset[str],
    ) -> tuple[_LiveEvent, ...]:
        if any(event._phase_value is not None for event in candidates.values()):
            return tuple(
                event
                for event in candidates.values()
                if event._phase_value is not None and event._phase_value in expected
            )
        return tuple(candidates.values())

    def _cleanup(self) -> tuple[BaseException, ...]:
        if self._cleanup_failures is not None:
            return self._cleanup_failures
        failures: list[BaseException] = []
        shot_id = self._shot_id
        if shot_id is not None:
            delete_client = self._cleanup_client or self._producer_client
            try:
                if not delete_client.delete('Shot', shot_id):
                    failures.append(RuntimeError(f'delete returned false for Shot:{shot_id}'))
            except BaseException as error:  # noqa: BLE001
                failures.append(error)
            try:
                residual = self._producer_client.find_one(
                    'Shot',
                    [
                        ['id', 'is', shot_id],
                        ['code', 'is', f'{self._namespace}-shot'],
                    ],
                    list(_SHOT_FIELDS),
                )
                if residual is not None:
                    failures.append(RuntimeError(f'residual Shot:{shot_id}'))
            except BaseException as error:  # noqa: BLE001
                failures.append(error)
        clients = (self._source_client, self._producer_client, self._cleanup_client)
        for client in clients:
            if client is None:
                continue
            try:
                client.close()
            except BaseException as error:  # noqa: BLE001
                failures.append(error)
        self._cleanup_failures = tuple(failures)
        return self._cleanup_failures

    def _correlate(
        self,
        candidates: Mapping[int, _LiveEvent],
        expected: frozenset[str],
    ) -> tuple[_LiveEvent, ...] | None:
        ordered = tuple(sorted(candidates.values(), key=lambda event: event._id))
        exposed = tuple(event for event in ordered if event._phase_value is not None)
        absent = tuple(event for event in ordered if event._phase_value is None)
        if exposed:
            if absent:
                raise RuntimeError('ambiguous mixed exposed and absent phase evidence')
            accepted = tuple(event for event in exposed if event._phase_value in expected)
            phases = tuple(cast('str', event._phase_value) for event in accepted)
            if len(phases) != len(set(phases)):
                raise RuntimeError('duplicate phase evidence for tracked Shot')
            if frozenset(phases) == expected:
                return accepted
            return None
        if len(absent) > len(expected):
            raise RuntimeError(
                'count-only event correlation has more matching rows than requested: '
                f'expected={len(expected)} observed={len(absent)}'
            )
        return absent if len(absent) == len(expected) else None

    def _create_shot(self) -> int:
        if self._shot_id is not None:
            raise RuntimeError('live site already owns a Shot')
        row = self._producer_client.create(
            'Shot',
            {'code': f'{self._namespace}-shot', 'project': dict(self._project)},
        )
        shot_id = row.get('id')
        if isinstance(shot_id, bool) or not isinstance(shot_id, int):
            raise TypeError('Shot creation returned an invalid id')
        self._shot_id = shot_id
        return shot_id

    def _event_matches(self, event: _LiveEvent, after_id: int) -> bool:
        return (
            self._shot_id is not None
            and event._id > after_id
            and event._value['event_type'] == 'Shotgun_Shot_Change'
            and event._value['attribute_name'] == 'description'
            and _event_entity_matches(event._value, self._shot_id)
        )

    def _merge_observed(self, events: Collection[_LiveEvent]) -> None:
        merged = {event._id: event for event in self._observed_events}
        merged.update((event._id, event) for event in events)
        self._observed_events = tuple(sorted(merged.values(), key=lambda event: event._id))

    def _scan_events(self, after_id: int) -> dict[int, _LiveEvent]:
        if self._shot_id is None:
            raise RuntimeError('create the live Shot before discovering events')
        filters: list[list[object]] = [
            ['entity', 'is', {'id': self._shot_id, 'type': 'Shot'}],
            ['event_type', 'is', 'Shotgun_Shot_Change'],
            ['attribute_name', 'is', 'description'],
            ['id', 'greater_than', after_id],
        ]
        candidates: dict[int, _LiveEvent] = {}
        page = 1
        while True:
            rows = self._source_client.find(
                'EventLogEntry',
                filters,
                list(_EVENT_FILTER_FIELDS),
                order=[{'direction': 'asc', 'field_name': 'id'}],
                limit=_EVENT_PAGE_LIMIT,
                page=page,
            )
            for row in rows:
                event = _live_event(row)
                if self._event_matches(event, after_id):
                    candidates[event._id] = event
            if len(rows) < _EVENT_PAGE_LIMIT:
                return candidates
            page += 1

    def _update_description(self, value: str) -> None:
        if self._shot_id is None:
            raise RuntimeError('create the live Shot before updating its description')
        self._producer_client.update('Shot', self._shot_id, {'description': value})

    @staticmethod
    def _valid_project(project: Mapping[str, object] | None, expected_name: str) -> bool:
        if project is None:
            return False
        project_id = project.get('id')
        return (
            project.get('type') == 'Project'
            and project.get('name') == expected_name
            and isinstance(project_id, int)
            and not isinstance(project_id, bool)
        )

    def _wait_for_events(
        self,
        *,
        after_id: int,
        expected_values: Iterable[str],
        timeout_secs: float,
    ) -> tuple[_LiveEvent, ...]:
        requested = tuple(expected_values)
        if not requested or len(requested) != len(set(requested)):
            raise ValueError('expected_values must be nonempty and unique')
        if self._shot_id is None:
            raise RuntimeError('create the live Shot before discovering events')
        expected = frozenset(requested)
        deadline = time.monotonic() + max(timeout_secs, 0.0)
        candidates: dict[int, _LiveEvent] = {}
        while True:
            candidates.update(self._scan_events(after_id))
            accepted = self._accepted_events(candidates, expected)
            self._merge_observed(accepted)
            result = self._correlate(candidates, expected)
            if result is not None:
                self._merge_observed(result)
                return result
            now = time.monotonic()
            if now >= deadline:
                raise RuntimeError(
                    f'event discovery timed out for Shot:{self._shot_id}: '
                    f'expected={len(expected)} observed={len(accepted)}'
                )
            time.sleep(min(0.05, deadline - now))


def _credential(
    environ: Mapping[str, str],
    *,
    key_name: str,
    script_name: str,
) -> _Credential:
    return _Credential(_script_name=environ[script_name], _api_key=environ[key_name])


def _event_entity_matches(event: Event, shot_id: int) -> bool:
    entity = event['entity']
    if not isinstance(entity, Mapping):
        return False
    entity_mapping = cast('Mapping[str, object]', entity)
    entity_id = entity_mapping.get('id')
    return entity_mapping.get('type') == 'Shot' and entity_id == shot_id and type(entity_id) is int


def _event_phase(event: Event) -> str | None:
    metadata = event['meta']
    if not isinstance(metadata, Mapping):
        return None
    value = cast('Mapping[str, object]', metadata).get('new_value')
    return value if isinstance(value, str) else None


def _live_event(row: Mapping[str, object]) -> _LiveEvent:
    event = Event(row)
    event_id = event['id']
    if isinstance(event_id, bool) or not isinstance(event_id, int):
        raise TypeError('live event id must be an integer')
    return _LiveEvent(_id=event_id, _phase_value=_event_phase(event), _value=event)


def _namespace() -> str:
    timestamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')
    return f'sg-event-daemon-{timestamp}-{uuid.uuid4().hex}'


def load_live_settings(
    options: _LiveOptions,
    environ: Mapping[str, str],
) -> _LiveSettings:
    required = (
        'SG_EVENT_TEST_PRODUCER_KEY',
        'SG_EVENT_TEST_PRODUCER_SCRIPT',
        'SG_EVENT_TEST_SOURCE_KEY',
        'SG_EVENT_TEST_SOURCE_SCRIPT',
        'SG_TEST_PROJECT',
        'SG_TEST_SITE',
    )
    for name in required:
        if not environ.get(name):
            raise pytest.UsageError(f'missing required live setting {name}')

    cleanup_key = environ.get('SG_EVENT_TEST_CLEANUP_KEY')
    cleanup_script = environ.get('SG_EVENT_TEST_CLEANUP_SCRIPT')
    if bool(cleanup_key) != bool(cleanup_script):
        raise pytest.UsageError('cleanup credentials must provide both Script and key')

    site = environ['SG_TEST_SITE']
    if urlsplit(site).hostname is None:
        raise pytest.UsageError('SG_TEST_SITE must include a hostname')

    cleanup = (
        _Credential(_script_name=cleanup_script, _api_key=cleanup_key)
        if cleanup_key and cleanup_script
        else None
    )
    return _LiveSettings(
        _site=site,
        _project=environ['SG_TEST_PROJECT'],
        _source=_credential(
            environ,
            key_name='SG_EVENT_TEST_SOURCE_KEY',
            script_name='SG_EVENT_TEST_SOURCE_SCRIPT',
        ),
        _producer=_credential(
            environ,
            key_name='SG_EVENT_TEST_PRODUCER_KEY',
            script_name='SG_EVENT_TEST_PRODUCER_SCRIPT',
        ),
        _cleanup=cleanup,
        _options=options,
    )


def open_provider_client(
    provider: _NamedProvider,
    site: str,
    credential: _Credential,
) -> _LiveClient:
    module = import_module('shotgun_api3')
    factory = getattr(module, 'Shotgun', None)
    if not callable(factory):
        raise ProviderError(f'provider {provider.name} does not export a callable Shotgun')
    try:
        client = factory(
            site,
            script_name=credential._script_name,
            api_key=credential._api_key,
            connect=False,
        )
    except Exception:  # noqa: BLE001
        raise ProviderError(f'provider {provider.name} could not open a Shotgun client') from None
    return cast('_LiveClient', client)


def validate_live_provider(options: _LiveOptions) -> Provider:
    required = {'async', 'sync'} if options._mode == 'sync+async' else {'sync'}
    return load_provider(options._provider, required)
