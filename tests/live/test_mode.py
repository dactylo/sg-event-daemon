import tomllib
from pathlib import Path
from typing import Literal

import pytest

_LIVE_ENVIRONMENT = {
    'SG_EVENT_TEST_CLEANUP_KEY': 'cleanup-key',
    'SG_EVENT_TEST_CLEANUP_SCRIPT': 'cleanup',
    'SG_EVENT_TEST_PRODUCER_KEY': 'producer-key',
    'SG_EVENT_TEST_PRODUCER_SCRIPT': 'producer',
    'SG_EVENT_TEST_SOURCE_KEY': 'source-key',
    'SG_EVENT_TEST_SOURCE_SCRIPT': 'reader',
    'SG_TEST_PROJECT': 'Live Acceptance',
    'SG_TEST_SITE': 'https://credentials-present.invalid',
}


def _set_all_live_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in _LIVE_ENVIRONMENT.items():
        monkeypatch.setenv(name, value)


def _write_isolated_live_test(
    pytester: pytest.Pytester,
    *,
    marker: Literal['live', 'live_load'],
    resolve_external: bool = False,
) -> None:
    repository_root = Path(__file__).parents[2]
    configuration = tomllib.loads((repository_root / 'pyproject.toml').read_text(encoding='utf-8'))
    markers = configuration['tool']['pytest']['ini_options']['markers']
    pytester.makeini('[pytest]\nmarkers =\n' + '\n'.join(f'    {marker}' for marker in markers))
    fixture_statement = (
        'return None'
        if resolve_external
        else "raise AssertionError('external fixture was resolved')"
    )
    pytester.makeconftest(
        f"""\
import sys

sys.path.insert(0, {str(repository_root)!r})
pytest_plugins = ('tests._live_mode',)
"""
    )
    pytester.makepyfile(
        test_external=f"""\
import pytest

pytestmark = pytest.mark.{marker}


@pytest.fixture
def external_resource() -> None:
    {fixture_statement}


def test_external(external_resource: None) -> None:
    assert external_resource is None
""",
    )


def test_credentials_do_not_enable_live_without_option(
    pytester: pytest.Pytester,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_all_live_environment(monkeypatch)
    _write_isolated_live_test(pytester, marker='live')
    result = pytester.runpytest_subprocess('-q')
    result.assert_outcomes(skipped=1)
    assert 'external fixture was resolved' not in result.stdout.str()


@pytest.mark.parametrize('count', ['1', '1001'])
def test_live_load_rejects_out_of_bounds_count(
    pytester: pytest.Pytester,
    count: str,
) -> None:
    _write_isolated_live_test(pytester, marker='live_load')
    result = pytester.runpytest_subprocess(
        '-q',
        '--run-live-load',
        '--live-provider=shotgun-api3',
        '--live-mode=sync',
        f'--live-load-events={count}',
    )
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(['*live load event count must be between 2 and 1000*'])


def test_load_option_does_not_select_durability(pytester: pytest.Pytester) -> None:
    _write_isolated_live_test(pytester, marker='live')
    result = pytester.runpytest_subprocess(
        '-q', '--run-live-load', '--live-provider=shotgun-api3', '--live-mode=sync'
    )
    result.assert_outcomes(skipped=1)


def test_run_live_requires_provider_and_mode(pytester: pytest.Pytester) -> None:
    _write_isolated_live_test(pytester, marker='live')
    result = pytester.runpytest_subprocess('-q', '--run-live')
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(['*--live-provider*--live-mode*required*'])


@pytest.mark.parametrize(
    ('marker', 'gate'),
    [
        ('live', '--run-live'),
        ('live_load', '--run-live-load'),
    ],
)
def test_selected_tier_resolves_external_fixture(
    pytester: pytest.Pytester,
    marker: Literal['live', 'live_load'],
    gate: str,
) -> None:
    _write_isolated_live_test(pytester, marker=marker, resolve_external=True)
    result = pytester.runpytest_subprocess(
        '-q',
        '--strict-markers',
        gate,
        '--live-provider=shotgun-api3',
        '--live-mode=sync',
    )
    result.assert_outcomes(passed=1)


def test_shotgun_api3_rejects_sync_async_mode(pytester: pytest.Pytester) -> None:
    _write_isolated_live_test(pytester, marker='live')
    result = pytester.runpytest_subprocess(
        '-q', '--run-live', '--live-provider=shotgun-api3', '--live-mode=sync+async'
    )
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(['*shotgun-api3*sync+async*invalid*'])
