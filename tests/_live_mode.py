import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import pytest

LiveMode = Literal['sync', 'sync+async']
LiveProvider = Literal['sg-python-api', 'shotgun-api3']


@dataclass(frozen=True)
class LiveOptions:
    provider: LiveProvider
    mode: LiveMode
    load_events: int
    report_dir: Path | None
    run_live: bool
    run_live_load: bool
    namespace: str | None = None
    recover: str | None = None


def _live_load_events(value: object) -> int:
    if not isinstance(value, str):
        raise pytest.UsageError('live load event count must be between 2 and 1000')
    try:
        count = int(value)
    except ValueError:
        raise pytest.UsageError('live load event count must be between 2 and 1000') from None
    if not 2 <= count <= 1_000:
        raise pytest.UsageError('live load event count must be between 2 and 1000')
    return count


def _live_mode(value: object) -> LiveMode:
    if value == 'sync':
        return 'sync'
    if value == 'sync+async':
        return 'sync+async'
    raise pytest.UsageError(
        '--live-provider and --live-mode are required; --live-mode must be sync or sync+async'
    )


def _live_namespace(value: object, option: str) -> str | None:
    if value is None:
        return None
    message = f'{option} must be a valid live namespace'
    if not isinstance(value, str):
        raise pytest.UsageError(message)
    match = re.fullmatch(r'sg-event-daemon-([0-9]{8}T[0-9]{12}Z)-[0-9a-f]{32}', value)
    if match is None:
        raise pytest.UsageError(message)
    try:
        datetime.strptime(match[1], '%Y%m%dT%H%M%S%fZ').replace(tzinfo=UTC)
    except ValueError:
        raise pytest.UsageError(message) from None
    return value


def _live_provider(value: object) -> LiveProvider:
    if value == 'sg-python-api':
        return 'sg-python-api'
    if value == 'shotgun-api3':
        return 'shotgun-api3'
    raise pytest.UsageError(
        '--live-provider and --live-mode are required; '
        '--live-provider must be sg-python-api or shotgun-api3'
    )


def _live_report_dir(value: object) -> Path | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise pytest.UsageError('--live-report-dir must name a path')
    return Path(value)


def live_options(config: pytest.Config) -> LiveOptions:
    provider = _live_provider(config.getoption('live_provider'))
    mode = _live_mode(config.getoption('live_mode'))
    if provider == 'shotgun-api3' and mode == 'sync+async':
        raise pytest.UsageError('shotgun-api3 with sync+async is invalid')
    return LiveOptions(
        provider=provider,
        mode=mode,
        load_events=_live_load_events(config.getoption('live_load_events')),
        report_dir=_live_report_dir(config.getoption('live_report_dir')),
        run_live=bool(config.getoption('run_live')),
        run_live_load=bool(config.getoption('run_live_load')),
        namespace=_live_namespace(config.getoption('live_namespace'), '--live-namespace'),
        recover=_live_namespace(config.getoption('live_recover'), '--live-recover'),
    )


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup('live acceptance')
    group.addoption(
        '--run-live',
        action='store_true',
        help='run external durability acceptance tests',
    )
    group.addoption(
        '--run-live-load',
        action='store_true',
        help='run external bounded-load acceptance tests',
    )
    group.addoption('--live-provider', help='live provider distribution identity')
    group.addoption('--live-mode', help='live sync or sync+async execution mode')
    group.addoption(
        '--live-load-events',
        default='1000',
        help='bounded live load event count from 2 through 1000',
    )
    group.addoption('--live-report-dir', help='optional sanitized report directory')
    group.addoption('--live-namespace', help="known namespace for this run's Shots")
    group.addoption('--live-recover', help='namespace whose Shots recovery deletes')


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    run_live = bool(config.getoption('run_live'))
    run_live_load = bool(config.getoption('run_live_load'))
    live_skip = pytest.mark.skip(reason='requires --run-live')
    live_load_skip = pytest.mark.skip(reason='requires --run-live-load')
    for item in items:
        if item.get_closest_marker('live') is not None and not run_live:
            item.add_marker(live_skip)
        if item.get_closest_marker('live_load') is not None and not run_live_load:
            item.add_marker(live_load_skip)


def pytest_configure(config: pytest.Config) -> None:
    _live_namespace(config.getoption('live_namespace'), '--live-namespace')
    _live_namespace(config.getoption('live_recover'), '--live-recover')
    if config.getoption('run_live') or config.getoption('run_live_load'):
        live_options(config)
