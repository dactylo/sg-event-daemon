from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib import import_module
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Protocol, cast
from urllib.parse import urlsplit

import pytest

from sg_event_daemon._events import EVENT_FIELDS, Event
from sg_event_daemon._providers import Provider, ProviderError, load_provider

if TYPE_CHECKING:
    from tests._live_mode import LiveOptions

_EVENT_FILTER_FIELDS = list(EVENT_FIELDS)
_EVENT_PAGE_LIMIT = 500
_PROJECT_FIELDS = ['id', 'name', 'type']
_SHOT_FIELDS = ['id', 'code', 'type']


@dataclass(frozen=True)
class Credential:
    script_name: str
    api_key: str = field(repr=False)


class LiveClient(Protocol):
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


class BulkClient(LiveClient, Protocol):
    def batch(self, requests: list[dict[str, object]]) -> list[dict[str, object]]: ...


class _NamedProvider(Protocol):
    @property
    def name(self) -> str: ...


@dataclass(frozen=True)
class LiveEvent:
    id: int
    phase_value: str | None
    value: Event


@dataclass(frozen=True)
class LiveSettings:
    site: str
    project: str
    source: Credential
    producer: Credential
    cleanup: Credential | None
    options: LiveOptions


class LiveSite:
    namespace: str
    observed_events: tuple[LiveEvent, ...]
    project: Mapping[str, object]
    shot_id: int | None

    def __init__(
        self,
        settings: LiveSettings,
        source_client: LiveClient,
        producer_client: LiveClient,
        cleanup_client: LiveClient | None = None,
    ) -> None:
        self.bulk_shot_ids: list[int] = []
        self._bulk_next = 0
        self._bulk_lock = threading.Lock()
        self._thread_clients: list[LiveClient] = []
        self._thread_local = threading.local()
        self._cleanup_client = cleanup_client
        self._cleanup_failures: tuple[BaseException, ...] | None = None
        self._producer_client = producer_client
        self._settings = settings
        self.source_client = source_client
        self.namespace = settings.options.namespace or _namespace()
        self.observed_events = ()
        self.shot_id = None
        project = producer_client.find_one(
            'Project',
            [['name', 'is', settings.project]],
            list(_PROJECT_FIELDS),
        )
        if not self._valid_project(project, settings.project):
            raise pytest.UsageError(f'Project {settings.project!r} was not found exactly')
        project_mapping = cast('dict[str, object]', project)
        self.project = MappingProxyType(dict(project_mapping))

    def _accepted_events(
        self,
        candidates: Mapping[int, LiveEvent],
        expected: frozenset[str],
    ) -> tuple[LiveEvent, ...]:
        if any(event.phase_value is not None for event in candidates.values()):
            return tuple(
                event
                for event in candidates.values()
                if event.phase_value is not None and event.phase_value in expected
            )
        return tuple(candidates.values())

    def _bulk_filters(self) -> list[list[object]]:
        return [
            ['project', 'is', {'type': 'Project', 'id': self.project['id']}],
            ['code', 'starts_with', f'{self.namespace}-bulk-'],
        ]

    def _cleanup_rows(
        self,
        filters: object,
        identifiers: set[int],
        label: str,
    ) -> list[BaseException]:
        failures: list[BaseException] = []
        client = self._cleanup_client or self._producer_client
        try:
            identifiers.update(
                cast('int', row['id']) for row in client.find('Shot', filters, ['id'])
            )
        except BaseException as error:  # noqa: BLE001
            failures.append(error)
        ordered = sorted(identifiers)
        for offset in range(0, len(ordered), 100):
            try:
                cast('BulkClient', client).batch(
                    [
                        {'request_type': 'delete', 'entity_type': 'Shot', 'entity_id': identifier}
                        for identifier in ordered[offset : offset + 100]
                    ]
                )
            except BaseException as error:  # noqa: BLE001
                failures.append(error)
        try:
            residuals = client.find('Shot', filters, ['id'])
            if residuals:
                failures.append(RuntimeError(f'residual Shot: {len(residuals)} {label} rows'))
        except BaseException as error:  # noqa: BLE001
            failures.append(error)
        return failures

    def _cleanup_bulk(self) -> list[BaseException]:
        if not self._bulk_next:
            return []
        return self._cleanup_rows(self._bulk_filters(), set(self.bulk_shot_ids), 'bulk')

    def _close_clients(self, failures: list[BaseException]) -> None:
        clients = (
            self.source_client,
            self._producer_client,
            self._cleanup_client,
            *self._thread_clients,
        )
        for client in clients:
            if client is None:
                continue
            try:
                client.close()
            except BaseException as error:  # noqa: BLE001
                failures.append(error)

    def _correlate(
        self,
        candidates: Mapping[int, LiveEvent],
        expected: frozenset[str],
    ) -> tuple[LiveEvent, ...] | None:
        ordered = tuple(sorted(candidates.values(), key=lambda event: event.id))
        exposed = tuple(event for event in ordered if event.phase_value is not None)
        absent = tuple(event for event in ordered if event.phase_value is None)
        if exposed:
            if absent:
                raise RuntimeError('ambiguous mixed exposed and absent phase evidence')
            accepted = tuple(event for event in exposed if event.phase_value in expected)
            phases = tuple(cast('str', event.phase_value) for event in accepted)
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

    def _event_matches(self, event: LiveEvent, after_id: int) -> bool:
        return (
            self.shot_id is not None
            and event.id > after_id
            and event.value['event_type'] == 'Shotgun_Shot_Change'
            and event.value['attribute_name'] == 'description'
            and _event_entity_matches(event.value, self.shot_id)
        )

    def _merge_observed(self, events: Collection[LiveEvent]) -> None:
        merged = {event.id: event for event in self.observed_events}
        merged.update((event.id, event) for event in events)
        self.observed_events = tuple(sorted(merged.values(), key=lambda event: event.id))

    def _scan_events(self, after_id: int) -> dict[int, LiveEvent]:
        if self.shot_id is None:
            raise RuntimeError('create the live Shot before discovering events')
        filters: list[list[object]] = [
            ['entity', 'is', {'id': self.shot_id, 'type': 'Shot'}],
            ['event_type', 'is', 'Shotgun_Shot_Change'],
            ['attribute_name', 'is', 'description'],
            ['id', 'greater_than', after_id],
        ]
        candidates: dict[int, LiveEvent] = {}
        page = 1
        while True:
            rows = self.source_client.find(
                'EventLogEntry',
                filters,
                list(_EVENT_FILTER_FIELDS),
                order=[{'direction': 'asc', 'field_name': 'id'}],
                limit=_EVENT_PAGE_LIMIT,
                page=page,
            )
            for row in rows:
                event = live_event(row)
                if self._event_matches(event, after_id):
                    candidates[event.id] = event
            if len(rows) < _EVENT_PAGE_LIMIT:
                return candidates
            page += 1

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

    def bulk_owned_ids(self) -> set[int]:
        client = self._cleanup_client or self._producer_client
        return set(self.bulk_shot_ids).union(
            cast('int', row['id']) for row in client.find('Shot', self._bulk_filters(), ['id'])
        )

    def cleanup(self) -> tuple[BaseException, ...]:
        if self._cleanup_failures is not None:
            return self._cleanup_failures
        failures: list[BaseException] = []
        failures.extend(self._cleanup_bulk())
        shot_id = self.shot_id
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
                        ['code', 'is', f'{self.namespace}-shot'],
                    ],
                    list(_SHOT_FIELDS),
                )
                if residual is not None:
                    failures.append(RuntimeError(f'residual Shot:{shot_id}'))
            except BaseException as error:  # noqa: BLE001
                failures.append(error)
        self._close_clients(failures)
        self._cleanup_failures = tuple(failures)
        return self._cleanup_failures

    def create_shot(self) -> int:
        if self.shot_id is not None:
            raise RuntimeError('live site already owns a Shot')
        row = self._producer_client.create(
            'Shot',
            {'code': f'{self.namespace}-shot', 'project': dict(self.project)},
        )
        shot_id = row.get('id')
        if isinstance(shot_id, bool) or not isinstance(shot_id, int):
            raise TypeError('Shot creation returned an invalid id')
        self.shot_id = shot_id
        return shot_id

    def create_shot_batch(self, count: int) -> tuple[int, ...]:
        with self._bulk_lock:
            start = self._bulk_next
            self._bulk_next += count
        client = cast('LiveClient', getattr(self._thread_local, 'client', self._producer_client))
        rows = cast('BulkClient', client).batch(
            [
                {
                    'request_type': 'create',
                    'entity_type': 'Shot',
                    'data': {
                        'code': f'{self.namespace}-bulk-{index}',
                        'project': {'type': 'Project', 'id': self.project['id']},
                    },
                }
                for index in range(start, start + count)
            ]
        )
        identifiers = tuple(cast('int', row['id']) for row in rows)
        with self._bulk_lock:
            self.bulk_shot_ids.extend(identifiers)
        return identifiers

    def open_thread_producer(self) -> LiveClient:
        client = open_provider_client(
            validate_live_provider(self._settings.options),
            self._settings.site,
            self._settings.producer,
        )
        with self._bulk_lock:
            self._thread_clients.append(client)
        self._thread_local.client = client
        return client

    def recover(self, namespace: str) -> tuple[BaseException, ...]:
        filters: list[object] = [
            ['project', 'is', {'type': 'Project', 'id': self.project['id']}],
            {
                'filter_operator': 'any',
                'filters': [
                    ['code', 'is', f'{namespace}-shot'],
                    ['code', 'starts_with', f'{namespace}-bulk-'],
                ],
            },
        ]
        failures = self._cleanup_rows(filters, set(), 'recovery')
        self._close_clients(failures)
        return tuple(failures)

    def update_description(self, value: str) -> None:
        if self.shot_id is None:
            raise RuntimeError('create the live Shot before updating its description')
        self._producer_client.update('Shot', self.shot_id, {'description': value})

    def wait_for_events(
        self,
        *,
        after_id: int,
        expected_values: Iterable[str],
        timeout_secs: float,
    ) -> tuple[LiveEvent, ...]:
        requested = tuple(expected_values)
        if not requested or len(requested) != len(set(requested)):
            raise ValueError('expected_values must be nonempty and unique')
        if self.shot_id is None:
            raise RuntimeError('create the live Shot before discovering events')
        expected = frozenset(requested)
        deadline = time.monotonic() + max(timeout_secs, 0.0)
        candidates: dict[int, LiveEvent] = {}
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
                    f'event discovery timed out for Shot:{self.shot_id}: '
                    f'expected={len(expected)} observed={len(accepted)}'
                )
            time.sleep(min(0.05, deadline - now))


def _credential(
    environ: Mapping[str, str],
    *,
    key_name: str,
    script_name: str,
) -> Credential:
    return Credential(script_name=environ[script_name], api_key=environ[key_name])


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


def _namespace() -> str:
    timestamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')
    return f'sg-event-daemon-{timestamp}-{uuid.uuid4().hex}'


def live_event(row: Mapping[str, object]) -> LiveEvent:
    event = Event(row)
    event_id = event['id']
    if isinstance(event_id, bool) or not isinstance(event_id, int):
        raise TypeError('live event id must be an integer')
    return LiveEvent(id=event_id, phase_value=_event_phase(event), value=event)


def load_live_settings(
    options: LiveOptions,
    environ: Mapping[str, str],
) -> LiveSettings:
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
        Credential(script_name=cleanup_script, api_key=cleanup_key)
        if cleanup_key and cleanup_script
        else None
    )
    return LiveSettings(
        site=site,
        project=environ['SG_TEST_PROJECT'],
        source=_credential(
            environ,
            key_name='SG_EVENT_TEST_SOURCE_KEY',
            script_name='SG_EVENT_TEST_SOURCE_SCRIPT',
        ),
        producer=_credential(
            environ,
            key_name='SG_EVENT_TEST_PRODUCER_KEY',
            script_name='SG_EVENT_TEST_PRODUCER_SCRIPT',
        ),
        cleanup=cleanup,
        options=options,
    )


def open_provider_client(
    provider: _NamedProvider,
    site: str,
    credential: Credential,
) -> LiveClient:
    module = import_module('shotgun_api3')
    factory = getattr(module, 'Shotgun', None)
    if not callable(factory):
        raise ProviderError(f'provider {provider.name} does not export a callable Shotgun')
    try:
        client = factory(
            site,
            script_name=credential.script_name,
            api_key=credential.api_key,
            connect=False,
        )
    except Exception:  # noqa: BLE001
        raise ProviderError(f'provider {provider.name} could not open a Shotgun client') from None
    cast('Any', client).config.timeout_secs = 30.0
    return cast('LiveClient', client)


def validate_live_provider(options: LiveOptions) -> Provider:
    required = {'async', 'sync'} if options.mode == 'sync+async' else {'sync'}
    return load_provider(options.provider, required)
