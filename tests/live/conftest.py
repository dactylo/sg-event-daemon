from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

import pytest

from tests._host_sampling import HostSampler
from tests._live_mode import live_options
from tests.live._daemon import (
    LIVE_SHUTDOWN_DEADLINE_SECS,
    LIVE_TIMEOUT_SECS,
    LiveCaseContext,
    LiveCaseFactory,
    LiveGroupSpec,
    LiveReport,
    OpenClientsError,
    SanitizedLiveError,
    SupervisorProcess,
    build_child_environment,
    validate_child_import,
    write_daemon_config,
    write_report,
)
from tests.live._site import (
    BulkClient,
    LiveClient,
    LiveEvent,
    LiveSettings,
    LiveSite,
    load_live_settings,
    open_provider_client,
    validate_live_provider,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sg_event_daemon._providers import Provider


class LiveHooks:
    def __init__(self, settings: LiveSettings, directory: Path, *, bulk: bool = False) -> None:
        self._child_directory = directory / 'unrelated-child-cwd'
        self._child_directory.mkdir(parents=True, mode=0o700)
        self._directory = directory
        self._bulk = bulk
        self.host_sampler = HostSampler(os.getpid()) if bulk else None
        environment = dict(os.environ)
        environment.pop('SG_EVENT_TEST_REQUEST_LOG', None)
        if bulk:
            environment['SG_EVENT_TEST_REQUEST_LOG'] = str(directory / 'requests.jsonl')
        self.environment = build_child_environment(
            settings,
            environment,
            (Path(__file__).resolve().parents[2] / 'src').resolve(),
        )
        self._provider: Provider | None = None
        self._settings = settings

    def build_process(self, config_path: Path) -> SupervisorProcess:
        return SupervisorProcess(
            config_path=config_path,
            environment=self.environment,
            directory=self._directory,
            timeout_secs=(3 if self._bulk else 1) * 2 * LIVE_SHUTDOWN_DEADLINE_SECS,
        )

    def cleanup(self, site: LiveSite) -> tuple[tuple[str, BaseException], ...]:
        categorized: list[tuple[str, BaseException]] = []
        for error in site.cleanup():
            if isinstance(error, _BoundaryError):
                categorized.append((error.category, error.original))
            elif 'residual Shot:' in str(error):
                categorized.append(('residual', error))
            elif 'delete returned false' in str(error):
                categorized.append(('delete', error))
            else:
                categorized.append(('cleanup', error))
        return tuple(categorized)

    def cleanup_clients(self, clients: object) -> tuple[tuple[str, BaseException], ...]:
        bundle = cast('_Clients', clients)
        failures: list[tuple[str, BaseException]] = []
        for category, client in (
            ('source-close', bundle.source),
            ('producer-close', bundle.producer),
            ('cleanup-close', bundle.cleanup),
        ):
            if client is None:
                continue
            try:
                client.close()
            except BaseException as error:  # noqa: BLE001
                original = error.original if isinstance(error, _BoundaryError) else error
                failures.append((category, original))
        return tuple(failures)

    def create_shot(self, site: LiveSite) -> int:
        return site.create_shot()

    def discover_baseline(self, site: LiveSite, value: str) -> LiveEvent:
        return site.wait_for_events(
            after_id=0,
            expected_values=(value,),
            timeout_secs=LIVE_TIMEOUT_SECS,
        )[0]

    def open_clients(self) -> object:
        provider = cast('Provider', self._provider)
        opened: list[tuple[str, LiveClient]] = []
        try:
            source_raw = open_provider_client(provider, self._settings.site, self._settings.source)
            opened.append(('source-close', source_raw))
            producer_raw = open_provider_client(
                provider,
                self._settings.site,
                self._settings.producer,
            )
            opened.append(('producer-close', producer_raw))
            cleanup_raw = (
                None
                if self._settings.cleanup is None
                else open_provider_client(
                    provider,
                    self._settings.site,
                    self._settings.cleanup,
                )
            )
            if cleanup_raw is not None:
                opened.append(('cleanup-close', cleanup_raw))
        except BaseException as error:  # noqa: BLE001
            failures: list[tuple[str, BaseException]] = []
            for category, client in opened:
                try:
                    client.close()
                except BaseException as close_error:  # noqa: BLE001
                    original = (
                        close_error.original
                        if isinstance(close_error, _BoundaryError)
                        else close_error
                    )
                    failures.append((category, original))
            raise OpenClientsError(error, tuple(failures)) from None
        return _Clients(
            cleanup=(
                None
                if cleanup_raw is None
                else cast(
                    'LiveClient', _CategorizedClient(cleanup_raw, close_category='cleanup-close')
                )
            ),
            producer=cast(
                'LiveClient',
                _CategorizedClient(producer_raw, close_category='producer-close'),
            ),
            source=cast(
                'LiveClient',
                _CategorizedClient(source_raw, close_category='source-close'),
            ),
        )

    def resolve_site(self, clients: object) -> LiveSite:
        bundle = cast('_Clients', clients)
        return LiveSite(
            self._settings,
            bundle.source,
            bundle.producer,
            bundle.cleanup,
        )

    def validate_child_import(self) -> None:
        validate_child_import(self.environment, self._child_directory)

    def validate_provider(self) -> None:
        if self.host_sampler is not None:
            self.host_sampler.start()
        self._provider = validate_live_provider(self._settings.options)

    def write_baseline(self, site: LiveSite) -> str:
        value = f'{site.namespace}-baseline'
        site.update_description(value)
        return value

    def write_config(
        self,
        site: LiveSite,
        shot_id: int,
    ) -> tuple[Path, Mapping[str, Path]]:
        paths = {
            'async': self._directory / 'async.jsonl',
            'config': self._directory / 'daemon.toml',
            'state': self._directory / 'state',
            'sync': self._directory / 'sync.jsonl',
        }
        if self._bulk:
            specs = (
                LiveGroupSpec('bulk-eager', 'sync', 'eager', 'bulk', 'dedicated'),
                LiveGroupSpec('bulk-strict', 'sync', 'strict', 'bulk', 'dedicated'),
            )
            paths.update({spec.name: self._directory / f'{spec.name}.jsonl' for spec in specs})
            config_path = write_daemon_config(
                paths,
                self._settings,
                shot_id=shot_id,
                groups=specs,
                late_event_window_secs=30.0,
                project_id=cast('int', site.project['id']),
                production_requests=True,
            )
            return config_path, {spec.name: paths[spec.name] for spec in specs}
        config_path = write_daemon_config(paths, self._settings, shot_id=shot_id)
        groups = ('sync',) if self._settings.options.mode == 'sync' else ('async', 'sync')
        return config_path, {group: paths[group] for group in groups}

    def write_report(
        self,
        report: LiveReport,
        failures: tuple[BaseException, ...],
    ) -> None:
        if self._bulk:
            sampler = self.host_sampler
            assert sampler is not None
            sampler.stop()
            cast('dict[str, object]', report.load_metrics)['host_samples'] = [
                asdict(sample) for sample in sampler.samples()
            ]
            report.site_hostname = '<redacted>'
            report.project_name = '<redacted>'
        write_report(
            report,
            self._settings.options.report_dir,
            failures=tuple(
                failure for failure in failures if isinstance(failure, SanitizedLiveError)
            ),
        )


class _BoundaryError(Exception):
    def __init__(self, category: str, original: BaseException) -> None:
        self.category = category
        self.original = original
        super().__init__(category)


class _CategorizedClient:
    def __init__(self, client: LiveClient, *, close_category: str) -> None:
        self._client = client
        self._close_category = close_category

    def batch(self, requests: list[dict[str, object]]) -> list[dict[str, object]]:
        return cast('BulkClient', self._client).batch(requests)

    def close(self) -> object:
        try:
            return self._client.close()
        except BaseException as error:  # noqa: BLE001
            raise _BoundaryError(self._close_category, error) from None

    def create(self, entity_type: str, data: dict[str, object]) -> dict[str, object]:
        return self._client.create(entity_type, data)

    def delete(self, entity_type: str, entity_id: int) -> bool:
        try:
            return self._client.delete(entity_type, entity_id)
        except BaseException as error:  # noqa: BLE001
            raise _BoundaryError('delete', error) from None

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **kwargs: object,
    ) -> list[dict[str, object]]:
        return self._client.find(entity_type, filters, fields, **kwargs)

    def find_one(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
    ) -> dict[str, object] | None:
        try:
            return self._client.find_one(entity_type, filters, fields)
        except BaseException as error:
            if entity_type == 'Shot':
                raise _BoundaryError('residual', error) from None
            raise

    def update(
        self,
        entity_type: str,
        entity_id: int,
        data: dict[str, object],
    ) -> dict[str, object]:
        return self._client.update(entity_type, entity_id, data)


@dataclass(frozen=True)
class _Clients:
    cleanup: LiveClient | None
    producer: LiveClient
    source: LiveClient


class _MarkedRequest(Protocol):
    config: pytest.Config
    node: pytest.Item


@pytest.fixture
def live_case_factory(request: pytest.FixtureRequest, tmp_path: Path) -> LiveCaseFactory:
    typed_request = cast('_MarkedRequest', request)
    item = typed_request.node
    is_live = item.get_closest_marker('live') is not None
    is_load = item.get_closest_marker('live_load') is not None
    selected = (is_live and typed_request.config.getoption('run_live')) or (
        is_load and typed_request.config.getoption('run_live_load')
    )
    if not selected:
        raise pytest.UsageError('live_case_factory requires an explicitly selected live tier')
    options = live_options(typed_request.config)
    settings = load_live_settings(options, os.environ)

    counter = 0

    def _factory(*, bulk: bool = False) -> LiveCaseContext:
        nonlocal counter
        directory = tmp_path / f'case-{counter}'
        counter += 1
        directory.mkdir(parents=True, mode=0o700)
        return LiveCaseContext(
            settings=settings,
            directory=directory,
            hooks=LiveHooks(settings, directory, bulk=bulk),
        )

    return _factory
