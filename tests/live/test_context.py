from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

import pytest

import tests.live.conftest as live_conftest
from sg_event_daemon._events import Event
from tests._live_mode import _LiveOptions
from tests.live._daemon import (
    _LiveCaseContext,
    _LiveReport,
    _SanitizedLiveError,
    _SupervisorProcess,
    write_report,
)
from tests.live._site import LiveSite, _Credential, _LiveEvent, _LiveSettings

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from tests.live._daemon import _ContextHooks


@dataclass
class _ContextHarness:
    _context: _LiveCaseContext
    _calls: list[str]
    _failures: set[str] = field(default_factory=set[str])
    _fail_at: str | None = None
    _report: _LiveReport | None = None
    _report_path: Path | None = None


class _FakeHooks:
    def __init__(self, directory: Path) -> None:
        self._directory = directory
        self._harness: _ContextHarness | None = None
        self._site = _FakeSite()

    def _require_harness(self) -> _ContextHarness:
        harness = self._harness
        assert harness is not None
        return harness

    def _build_process(self, config_path: Path) -> _SupervisorProcess:
        del config_path
        return cast('_SupervisorProcess', _FakeProcess(self._require_harness()))

    def _call(self, name: str) -> None:
        harness = self._require_harness()
        harness._calls.append(name)
        if harness._fail_at == name:
            raise RuntimeError(f'{name} failed with source-key')

    def _cleanup(self, site: LiveSite) -> tuple[tuple[str, BaseException], ...]:
        del site
        harness = self._require_harness()
        failures: list[tuple[str, BaseException]] = []
        if self._site._shot_id is not None:
            for call, category in (
                ('delete-shot', 'delete'),
                ('verify-absent', 'residual'),
            ):
                harness._calls.append(call)
                if call in harness._failures:
                    failures.append((category, RuntimeError(f'{call} failed')))
        for call, category in (
            ('close-source', 'source-close'),
            ('close-producer', 'producer-close'),
            ('close-cleanup', 'cleanup-close'),
        ):
            harness._calls.append(call)
            if call in harness._failures:
                failures.append((category, RuntimeError(f'{call} failed')))
        return tuple(failures)

    def _cleanup_clients(self, clients: object) -> tuple[tuple[str, BaseException], ...]:
        del clients
        harness = self._require_harness()
        failures: list[tuple[str, BaseException]] = []
        for call, category in (
            ('close-source', 'source-close'),
            ('close-producer', 'producer-close'),
            ('close-cleanup', 'cleanup-close'),
        ):
            harness._calls.append(call)
            if call in harness._failures:
                failures.append((category, RuntimeError(f'{call} failed')))
        return tuple(failures)

    def _create_shot(self, site: LiveSite) -> int:
        del site
        self._call('create-shot')
        self._site._shot_id = 41
        return 41

    def _discover_baseline(self, site: LiveSite, value: str) -> _LiveEvent:
        del site, value
        self._call('discover-baseline')
        event = Event(
            {
                'attribute_name': 'description',
                'created_at': '2026-08-10T12:00:00Z',
                'entity': {'id': 41, 'type': 'Shot'},
                'event_type': 'Shotgun_Shot_Change',
                'id': 100,
                'meta': {'new_value': 'baseline'},
                'project': {'id': 7, 'type': 'Project'},
                'session_uuid': None,
                'type': 'EventLogEntry',
                'user': None,
            }
        )
        return _LiveEvent(_id=100, _phase_value='baseline', _value=event)

    def _open_clients(self) -> object:
        self._call('open-clients')
        return object()

    def _resolve_site(self, clients: object) -> LiveSite:
        del clients
        self._call('resolve-project')
        return cast('LiveSite', self._site)

    def _validate_child_import(self) -> None:
        self._call('validate-child-import')

    def _validate_provider(self) -> None:
        self._call('validate-provider')

    def _write_baseline(self, site: LiveSite) -> str:
        del site
        self._call('write-baseline')
        return 'context-test-baseline'

    def _write_config(
        self,
        site: LiveSite,
        shot_id: int,
    ) -> tuple[Path, Mapping[str, Path]]:
        del site, shot_id
        self._call('write-config')
        return self._directory / 'context-config.toml', MappingProxyType(
            {'sync': self._directory / 'context-sync.jsonl'}
        )

    def _write_report(
        self,
        report: _LiveReport,
        failures: tuple[BaseException, ...],
    ) -> None:
        harness = self._require_harness()
        harness._report = report
        self._call('write-report')
        harness._report_path = write_report(
            report,
            self._directory / 'reports',
            failures=failures,
            terminal=None,
        )


class _FakeProcess:
    def __init__(self, harness: _ContextHarness) -> None:
        self._harness = harness

    def _stop(self) -> tuple[BaseException, ...]:
        self._harness._calls.append('stop-process')
        if 'stop-process' in self._harness._failures:
            return (RuntimeError('stuck process'),)
        return ()


class _FakeSite:
    _namespace = 'context-test'
    _observed_events: tuple[_LiveEvent, ...] = ()
    _shot_id: int | None = None


class _PartialClient:
    def __init__(self, close_failure: BaseException) -> None:
        self._close_count = 0
        self._close_failure = close_failure

    def close(self) -> None:
        self._close_count += 1
        raise self._close_failure


def _categories(group: BaseExceptionGroup[BaseException]) -> list[str]:
    result: list[str] = []
    for failure in group.exceptions:
        assert isinstance(failure, _SanitizedLiveError)
        result.append(failure._category)
    return result


def _settings() -> _LiveSettings:
    return _LiveSettings(
        _site='https://example.invalid/private/path',
        _project='Live Acceptance',
        _source=_Credential(_script_name='source', _api_key='source-key'),
        _producer=_Credential(_script_name='producer', _api_key='producer-key'),
        _cleanup=_Credential(_script_name='cleanup', _api_key='cleanup-key'),
        _options=_LiveOptions(
            _provider='sg-python-api',
            _mode='sync',
            _load_events=1_000,
            _report_dir=None,
            _run_live=True,
            _run_live_load=False,
        ),
    )


def _live_hooks(directory: Path) -> _ContextHooks:
    hooks_factory = cast(
        'Callable[[_LiveSettings, Path], _ContextHooks]',
        vars(live_conftest)['_LiveHooks'],
    )
    return hooks_factory(_settings(), directory)


@pytest.fixture
def harness(tmp_path: Path) -> _ContextHarness:
    hooks = _FakeHooks(tmp_path)
    result = _ContextHarness(
        _context=_LiveCaseContext(settings=_settings(), directory=tmp_path, hooks=hooks),
        _calls=[],
    )
    hooks._harness = result
    return result


def test_context_cleans_up_partial_enter_failure(harness: _ContextHarness) -> None:
    harness._fail_at = 'create-shot'

    with (
        pytest.raises(BaseExceptionGroup, match='live acceptance setup') as caught,
        harness._context,
    ):
        pytest.fail('body must not run')

    assert _categories(caught.value) == ['setup']
    assert harness._calls[-4:] == [
        'close-source',
        'close-producer',
        'close-cleanup',
        'write-report',
    ]


def test_context_cleanup_is_idempotent(harness: _ContextHarness) -> None:
    with harness._context:
        pass

    assert harness._context._finalize() == ()
    assert harness._calls.count('stop-process') == 1
    assert harness._calls.count('write-report') == 1


def test_context_closes_open_clients_when_project_resolution_fails(
    harness: _ContextHarness,
) -> None:
    harness._fail_at = 'resolve-project'
    harness._failures.add('close-source')

    with (
        pytest.raises(BaseExceptionGroup, match='live acceptance setup') as caught,
        harness._context,
    ):
        pytest.fail('body must not run')

    assert _categories(caught.value) == ['setup', 'source-close']
    assert harness._calls[-4:] == [
        'close-source',
        'close-producer',
        'close-cleanup',
        'write-report',
    ]


@pytest.mark.parametrize('control', [KeyboardInterrupt(), asyncio.CancelledError()])
def test_context_preserves_process_control_failure_identity(
    harness: _ContextHarness,
    control: BaseException,
) -> None:
    harness._failures.add('close-source')

    with pytest.raises(BaseExceptionGroup) as caught, harness._context:
        raise control

    assert caught.value.exceptions[0] is control
    assert isinstance(caught.value.exceptions[1], _SanitizedLiveError)
    assert harness._calls[-1] == 'write-report'


def test_context_rejects_child_import_before_external_setup(harness: _ContextHarness) -> None:
    harness._fail_at = 'validate-child-import'

    with pytest.raises(BaseExceptionGroup, match='live acceptance setup'), harness._context:
        pytest.fail('body must not run')

    assert harness._calls == ['validate-provider', 'validate-child-import', 'write-report']


def test_context_report_uses_hostname_and_partial_observed_events(
    harness: _ContextHarness,
) -> None:
    report: _LiveReport | None = None
    with harness._context as live_case:
        live_case._site._observed_events = (
            live_case._baseline_event,
            _LiveEvent(_id=101, _phase_value='one', _value=live_case._baseline_event._value),
        )
        report = live_case._report

    assert report is not None
    assert report.site_hostname == 'example.invalid'
    assert report.expected_ids == [101]
    assert report.source_visible_count == 1


def test_context_retains_body_and_every_finalizer_failure(harness: _ContextHarness) -> None:
    harness._failures.update(
        {
            'stop-process',
            'delete-shot',
            'verify-absent',
            'close-source',
            'close-producer',
            'close-cleanup',
        }
    )

    with pytest.raises(BaseExceptionGroup) as caught, harness._context:
        raise AssertionError('primary failure')

    assert _categories(caught.value) == [
        'test',
        'shutdown',
        'delete',
        'residual',
        'source-close',
        'producer-close',
        'cleanup-close',
    ]
    assert harness._calls[-1] == 'write-report'


def test_context_retains_cleanup_outcome_when_report_write_fails(
    harness: _ContextHarness,
) -> None:
    harness._fail_at = 'write-report'

    with pytest.raises(BaseExceptionGroup) as caught, harness._context:
        pass

    assert _categories(caught.value) == ['report']
    assert harness._report is not None
    assert harness._report.cleanup_outcome == ['shutdown:succeeded', 'cleanup:succeeded']
    assert harness._calls[-1] == 'write-report'


def test_context_retains_partial_open_and_ordinary_close_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    open_failure = RuntimeError('cleanup open failed')
    source_close_failure = RuntimeError('source close failed')
    producer_close_failure = RuntimeError('producer close failed')
    source = _PartialClient(source_close_failure)
    producer = _PartialClient(producer_close_failure)
    calls = 0

    def _open_client(*_args: object) -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            return source
        if calls == 2:
            return producer
        raise open_failure

    def _validate_import(*_args: object) -> None:
        return None

    def _validate_provider(_options: _LiveOptions) -> object:
        return object()

    monkeypatch.setattr(live_conftest, 'open_provider_client', _open_client)
    monkeypatch.setattr(live_conftest, 'validate_child_import', _validate_import)
    monkeypatch.setattr(live_conftest, 'validate_live_provider', _validate_provider)
    hooks = _live_hooks(tmp_path)
    context = _LiveCaseContext(settings=_settings(), directory=tmp_path, hooks=hooks)

    with pytest.raises(BaseExceptionGroup, match='live acceptance setup') as caught, context:
        pytest.fail('body must not run')

    assert _categories(caught.value) == ['setup', 'source-close', 'producer-close']
    assert cast('_SanitizedLiveError', caught.value.exceptions[0])._original is open_failure
    assert (
        cast('_SanitizedLiveError', caught.value.exceptions[1])._original is source_close_failure
    )
    assert (
        cast('_SanitizedLiveError', caught.value.exceptions[2])._original is producer_close_failure
    )
    assert source._close_count == 1
    assert producer._close_count == 1


def test_context_retains_partial_open_control_failure_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    open_failure = KeyboardInterrupt()
    close_failure = asyncio.CancelledError()
    source = _PartialClient(close_failure)
    calls = 0

    def _open_client(*_args: object) -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            return source
        raise open_failure

    def _validate_import(*_args: object) -> None:
        return None

    def _validate_provider(_options: _LiveOptions) -> object:
        return object()

    monkeypatch.setattr(live_conftest, 'open_provider_client', _open_client)
    monkeypatch.setattr(live_conftest, 'validate_child_import', _validate_import)
    monkeypatch.setattr(live_conftest, 'validate_live_provider', _validate_provider)
    hooks = _live_hooks(tmp_path)
    context = _LiveCaseContext(settings=_settings(), directory=tmp_path, hooks=hooks)

    with pytest.raises(BaseExceptionGroup, match='live acceptance setup') as caught, context:
        pytest.fail('body must not run')

    assert caught.value.exceptions == (open_failure, close_failure)
    assert source._close_count == 1


def test_context_runs_successful_finalizers_in_order(harness: _ContextHarness) -> None:
    with harness._context:
        harness._calls.append('body')

    assert harness._calls == [
        'validate-provider',
        'validate-child-import',
        'open-clients',
        'resolve-project',
        'create-shot',
        'write-baseline',
        'discover-baseline',
        'write-config',
        'body',
        'stop-process',
        'delete-shot',
        'verify-absent',
        'close-source',
        'close-producer',
        'close-cleanup',
        'write-report',
    ]


def test_context_serializes_failed_cleanup_outcome(harness: _ContextHarness) -> None:
    harness._failures.add('delete-shot')

    with pytest.raises(BaseExceptionGroup) as caught, harness._context:
        pass

    assert _categories(caught.value) == ['delete']
    assert harness._report_path is not None
    payload = json.loads(harness._report_path.read_text(encoding='utf-8'))
    assert payload['cleanup_outcome'] == ['shutdown:succeeded', 'cleanup:failed']
    assert payload['failure_categories'] == ['delete']


def test_context_serializes_successful_cleanup_outcome(harness: _ContextHarness) -> None:
    with harness._context:
        pass

    assert harness._report_path is not None
    payload = json.loads(harness._report_path.read_text(encoding='utf-8'))
    assert payload['cleanup_outcome'] == ['shutdown:succeeded', 'cleanup:succeeded']


def test_live_failure_and_report_accept_single_pass_values(tmp_path: Path) -> None:
    failure = _SanitizedLiveError(
        'setup',
        RuntimeError('secret value'),
        (secret for secret in ('secret',)),
    )
    report = _LiveReport(
        site_hostname='example.invalid',
        project_name='Live Acceptance',
        provider='sg-python-api',
        mode='sync',
        namespace='acceptance-run',
    )

    path = write_report(
        report,
        tmp_path,
        failures=(item for item in (failure,)),
        terminal=None,
    )

    assert '<redacted>' in str(failure)
    assert report.failure_categories == ['setup']
    assert path is not None
    assert path.is_file()
