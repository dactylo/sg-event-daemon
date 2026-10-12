from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

import pytest

from tests._live_mode import _live_options
from tests.live._daemon import (
    _LIVE_TIMEOUT_SECS,
    LiveCaseFactory,
    _LiveCaseContext,
    _LiveReport,
    _OpenClientsError,
    _SanitizedLiveError,
    _SupervisorProcess,
    build_child_environment,
    validate_child_import,
    write_daemon_config,
    write_report,
)
from tests.live._site import (
    LiveSite,
    _LiveClient,
    _LiveEvent,
    _LiveSettings,
    load_live_settings,
    open_provider_client,
    validate_live_provider,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sg_event_daemon._providers import Provider


class _BoundaryError(Exception):
    def __init__(self, category: str, original: BaseException) -> None:
        self._category = category
        self._original = original
        super().__init__(category)


class _CategorizedClient:
    def __init__(self, client: _LiveClient, *, close_category: str) -> None:
        self._client = client
        self._close_category = close_category

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
    _cleanup: _LiveClient | None
    _producer: _LiveClient
    _source: _LiveClient


class _LiveHooks:
    def __init__(self, settings: _LiveSettings, directory: Path) -> None:
        self._child_directory = directory / 'unrelated-child-cwd'
        self._child_directory.mkdir(parents=True, mode=0o700)
        self._directory = directory
        self._environment = build_child_environment(
            settings,
            os.environ,
            (Path(__file__).resolve().parents[2] / 'src').resolve(),
        )
        self._provider: Provider | None = None
        self._settings = settings

    def _build_process(self, config_path: Path) -> _SupervisorProcess:
        return _SupervisorProcess(
            config_path=config_path,
            environment=self._environment,
            directory=self._directory,
            timeout_secs=5.0,
        )

    def _cleanup(self, site: LiveSite) -> tuple[tuple[str, BaseException], ...]:
        categorized: list[tuple[str, BaseException]] = []
        close_categories = iter(('source-close', 'producer-close', 'cleanup-close'))
        for error in site._cleanup():
            if isinstance(error, _BoundaryError):
                categorized.append((error._category, error._original))
            elif 'residual Shot:' in str(error):
                categorized.append(('residual', error))
            elif 'delete returned false' in str(error):
                categorized.append(('delete', error))
            else:
                categorized.append((next(close_categories, 'cleanup'), error))
        return tuple(categorized)

    def _cleanup_clients(self, clients: object) -> tuple[tuple[str, BaseException], ...]:
        bundle = cast('_Clients', clients)
        failures: list[tuple[str, BaseException]] = []
        for category, client in (
            ('source-close', bundle._source),
            ('producer-close', bundle._producer),
            ('cleanup-close', bundle._cleanup),
        ):
            if client is None:
                continue
            try:
                client.close()
            except BaseException as error:  # noqa: BLE001
                original = error._original if isinstance(error, _BoundaryError) else error
                failures.append((category, original))
        return tuple(failures)

    def _create_shot(self, site: LiveSite) -> int:
        return site._create_shot()

    def _discover_baseline(self, site: LiveSite, value: str) -> _LiveEvent:
        return site._wait_for_events(
            after_id=0,
            expected_values=(value,),
            timeout_secs=_LIVE_TIMEOUT_SECS,
        )[0]

    def _open_clients(self) -> object:
        provider = cast('Provider', self._provider)
        opened: list[tuple[str, _LiveClient]] = []
        try:
            source_raw = open_provider_client(
                provider, self._settings._site, self._settings._source
            )
            opened.append(('source-close', source_raw))
            producer_raw = open_provider_client(
                provider,
                self._settings._site,
                self._settings._producer,
            )
            opened.append(('producer-close', producer_raw))
            cleanup_raw = (
                None
                if self._settings._cleanup is None
                else open_provider_client(
                    provider,
                    self._settings._site,
                    self._settings._cleanup,
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
                        close_error._original
                        if isinstance(close_error, _BoundaryError)
                        else close_error
                    )
                    failures.append((category, original))
            raise _OpenClientsError(error, tuple(failures)) from None
        return _Clients(
            _cleanup=(
                None
                if cleanup_raw is None
                else cast(
                    '_LiveClient', _CategorizedClient(cleanup_raw, close_category='cleanup-close')
                )
            ),
            _producer=cast(
                '_LiveClient',
                _CategorizedClient(producer_raw, close_category='producer-close'),
            ),
            _source=cast(
                '_LiveClient',
                _CategorizedClient(source_raw, close_category='source-close'),
            ),
        )

    def _resolve_site(self, clients: object) -> LiveSite:
        bundle = cast('_Clients', clients)
        return LiveSite(
            self._settings,
            bundle._source,
            bundle._producer,
            bundle._cleanup,
        )

    def _validate_child_import(self) -> None:
        validate_child_import(self._environment, self._child_directory)

    def _validate_provider(self) -> None:
        self._provider = validate_live_provider(self._settings._options)

    def _write_baseline(self, site: LiveSite) -> str:
        value = f'{site._namespace}-baseline'
        site._update_description(value)
        return value

    def _write_config(
        self,
        site: LiveSite,
        shot_id: int,
    ) -> tuple[Path, Mapping[str, Path]]:
        del site
        paths = {
            'async': self._directory / 'async.jsonl',
            'config': self._directory / 'daemon.toml',
            'state': self._directory / 'state',
            'sync': self._directory / 'sync.jsonl',
        }
        config_path = write_daemon_config(paths, self._settings, shot_id=shot_id)
        groups = ('sync',) if self._settings._options._mode == 'sync' else ('async', 'sync')
        return config_path, {group: paths[group] for group in groups}

    def _write_report(
        self,
        report: _LiveReport,
        failures: tuple[BaseException, ...],
    ) -> None:
        write_report(
            report,
            self._settings._options._report_dir,
            failures=tuple(
                failure for failure in failures if isinstance(failure, _SanitizedLiveError)
            ),
        )


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
    options = _live_options(typed_request.config)
    settings = load_live_settings(options, os.environ)

    counter = 0

    def _factory() -> _LiveCaseContext:
        nonlocal counter
        directory = tmp_path / f'case-{counter}'
        counter += 1
        directory.mkdir(parents=True, mode=0o700)
        return _LiveCaseContext(
            settings=settings,
            directory=directory,
            hooks=_LiveHooks(settings, directory),
        )

    return _factory
