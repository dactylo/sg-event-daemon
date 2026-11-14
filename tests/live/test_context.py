from __future__ import annotations

import asyncio
import json
import os
import select
import signal
import subprocess
import sys
import threading
import tomllib
from dataclasses import dataclass, field, replace
from types import MappingProxyType, SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

import tests._host_sampling as host_sampling
import tests.live.conftest as live_conftest
from sg_event_daemon._config import load_config
from sg_event_daemon._events import Event
from sg_event_daemon._worker import build_worker_specs
from tests._host_sampling import HostSample
from tests._live_mode import LiveOptions
from tests.live._daemon import (
    LiveCaseContext,
    LiveReport,
    SanitizedLiveError,
    SupervisorProcess,
    write_report,
)
from tests.live._site import Credential, LiveEvent, LiveSettings, LiveSite

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from pathlib import Path

    from tests._live_mode import LiveMode
    from tests.live._daemon import ContextHooks


@dataclass
class _ContextHarness:
    context: LiveCaseContext
    calls: list[str]
    failures: set[str] = field(default_factory=set[str])
    fail_at: str | None = None
    report: LiveReport | None = None
    report_path: Path | None = None


class _FakeHooks:
    def __init__(self, directory: Path) -> None:
        self._directory = directory
        self.harness: _ContextHarness | None = None
        self._site = _FakeSite()

    def _require_harness(self) -> _ContextHarness:
        harness = self.harness
        assert harness is not None
        return harness

    def _call(self, name: str) -> None:
        harness = self._require_harness()
        harness.calls.append(name)
        if harness.fail_at == name:
            raise RuntimeError(f'{name} failed with source-key')

    def build_process(self, config_path: Path) -> SupervisorProcess:
        del config_path
        return cast('SupervisorProcess', _FakeProcess(self._require_harness()))

    def cleanup(self, site: LiveSite) -> tuple[tuple[str, BaseException], ...]:
        del site
        harness = self._require_harness()
        failures: list[tuple[str, BaseException]] = []
        if self._site.shot_id is not None:
            for call, category in (
                ('delete-shot', 'delete'),
                ('verify-absent', 'residual'),
            ):
                harness.calls.append(call)
                if call in harness.failures:
                    failures.append((category, RuntimeError(f'{call} failed')))
        for call, category in (
            ('close-source', 'source-close'),
            ('close-producer', 'producer-close'),
            ('close-cleanup', 'cleanup-close'),
        ):
            harness.calls.append(call)
            if call in harness.failures:
                failures.append((category, RuntimeError(f'{call} failed')))
        return tuple(failures)

    def cleanup_clients(self, clients: object) -> tuple[tuple[str, BaseException], ...]:
        del clients
        harness = self._require_harness()
        failures: list[tuple[str, BaseException]] = []
        for call, category in (
            ('close-source', 'source-close'),
            ('close-producer', 'producer-close'),
            ('close-cleanup', 'cleanup-close'),
        ):
            harness.calls.append(call)
            if call in harness.failures:
                failures.append((category, RuntimeError(f'{call} failed')))
        return tuple(failures)

    def create_shot(self, site: LiveSite) -> int:
        del site
        self._call('create-shot')
        self._site.shot_id = 41
        return 41

    def discover_baseline(self, site: LiveSite, value: str) -> LiveEvent:
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
        return LiveEvent(id=100, phase_value='baseline', value=event)

    def open_clients(self) -> object:
        self._call('open-clients')
        return object()

    def resolve_site(self, clients: object) -> LiveSite:
        del clients
        self._call('resolve-project')
        return cast('LiveSite', self._site)

    def validate_child_import(self) -> None:
        self._call('validate-child-import')

    def validate_provider(self) -> None:
        self._call('validate-provider')

    def write_baseline(self, site: LiveSite) -> str:
        del site
        self._call('write-baseline')
        return 'context-test-baseline'

    def write_config(
        self,
        site: LiveSite,
        shot_id: int,
    ) -> tuple[Path, Mapping[str, Path]]:
        del site, shot_id
        self._call('write-config')
        return self._directory / 'context-config.toml', MappingProxyType(
            {'sync': self._directory / 'context-sync.jsonl'}
        )

    def write_report(
        self,
        report: LiveReport,
        failures: tuple[BaseException, ...],
    ) -> None:
        harness = self._require_harness()
        harness.report = report
        self._call('write-report')
        harness.report_path = write_report(
            report,
            self._directory / 'reports',
            failures=failures,
            terminal=None,
        )


class _FakeProcess:
    def __init__(self, harness: _ContextHarness) -> None:
        self._harness = harness

    def stop(self) -> tuple[BaseException, ...]:
        self._harness.calls.append('stop-process')
        if 'stop-process' in self._harness.failures:
            return (RuntimeError('stuck process'),)
        return ()


class _FakeSite:
    namespace = 'context-test'
    observed_events: tuple[LiveEvent, ...] = ()
    shot_id: int | None = None


class _PartialClient:
    def __init__(self, close_failure: BaseException) -> None:
        self.close_count = 0
        self._close_failure = close_failure

    def close(self) -> None:
        self.close_count += 1
        raise self._close_failure


def _categories(group: BaseExceptionGroup[BaseException]) -> list[str]:
    result: list[str] = []
    for failure in group.exceptions:
        assert isinstance(failure, SanitizedLiveError)
        result.append(failure.category)
    return result


def _settings() -> LiveSettings:
    return LiveSettings(
        site='https://example.invalid/private/path',
        project='Live Acceptance',
        source=Credential(script_name='source', api_key='source-key'),
        producer=Credential(script_name='producer', api_key='producer-key'),
        cleanup=Credential(script_name='cleanup', api_key='cleanup-key'),
        options=LiveOptions(
            provider='sg-python-api',
            mode='sync',
            load_events=1_000,
            report_dir=None,
            run_live=True,
            run_live_load=False,
        ),
    )


def _live_hooks(directory: Path) -> ContextHooks:
    hooks_factory = cast(
        'Callable[[LiveSettings, Path], ContextHooks]',
        vars(live_conftest)['LiveHooks'],
    )
    return hooks_factory(_settings(), directory)


@pytest.fixture
def harness(tmp_path: Path) -> _ContextHarness:
    hooks = _FakeHooks(tmp_path)
    result = _ContextHarness(
        context=LiveCaseContext(settings=_settings(), directory=tmp_path, hooks=hooks),
        calls=[],
    )
    hooks.harness = result
    return result


def _bulk_host_harness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    real_sampler: bool = False,
    expected_samples: int = 2,
) -> tuple[_ContextHarness, list[str]]:
    calls: list[str] = []
    samples: list[HostSample] = []

    class FakeSampler:
        def __init__(self, root_pid: int, *, interval_secs: float = 5.0) -> None:
            assert root_pid == os.getpid()
            assert interval_secs == 5.0

        def start(self) -> None:
            calls.append('sampler-start')
            samples.append(HostSample(1.0, 10.0, 20.0, 1.0, (), None))

        def stop(self) -> None:
            calls.append('sampler-stop')
            samples.append(HostSample(3.0, 12.0, 95.0, 2.0, (), None))

        def samples(self) -> tuple[HostSample, ...]:
            return tuple(samples)

    monkeypatch.setattr(
        live_conftest,
        'HostSampler',
        host_sampling.HostSampler if real_sampler else FakeSampler,
    )
    fake = _FakeHooks(tmp_path)
    hooks = live_conftest.LiveHooks(_settings(), tmp_path, bulk=True)
    harness = _ContextHarness(
        context=LiveCaseContext(settings=_settings(), directory=tmp_path, hooks=hooks),
        calls=calls,
    )
    fake.harness = harness
    for name in (
        'build_process',
        'cleanup',
        'cleanup_clients',
        'create_shot',
        'discover_baseline',
        'open_clients',
        'resolve_site',
        'validate_child_import',
        'write_baseline',
        'write_config',
    ):
        monkeypatch.setattr(hooks, name, getattr(fake, name))

    def validate(_options: LiveOptions) -> object:
        calls.append('validate-provider')
        return object()

    def write(
        report: LiveReport, _directory: Path | None, *, failures: tuple[BaseException, ...]
    ) -> None:
        calls.append('write-report')
        rows = json.loads(json.dumps(report.load_metrics['host_samples']))
        if real_sampler:
            assert len(rows) == expected_samples
            assert all(row['busy_percent'] == 20.0 for row in rows)
            harness.report_path = write_report(
                report, tmp_path / 'reports', failures=failures, terminal=None
            )
            return
        assert rows == [
            {
                'monotonic': 1.0,
                'wall_time': 10.0,
                'busy_percent': 20.0,
                'load_average_1m': 1.0,
                'outside_processes': [],
                'error': None,
            },
            {
                'monotonic': 3.0,
                'wall_time': 12.0,
                'busy_percent': 95.0,
                'load_average_1m': 2.0,
                'outside_processes': [],
                'error': None,
            },
        ]

    monkeypatch.setattr(live_conftest, 'validate_live_provider', validate)
    monkeypatch.setattr(live_conftest, 'write_report', write)
    return harness, calls


def _interrupt_child(directory: Path) -> None:
    hooks = _FakeHooks(directory)
    context = LiveCaseContext(settings=_settings(), directory=directory, hooks=hooks)
    harness = _ContextHarness(context=context, calls=[])
    hooks.harness = harness
    try:
        with context:
            read_fd, write_fd = os.pipe()
            try:
                os.set_blocking(write_fd, False)
                previous_wakeup_fd = signal.set_wakeup_fd(write_fd)
                try:
                    # Register the buffered wakeup before publishing readiness, so
                    # a signal arriving before select still makes the pipe readable.
                    print('shot-created', flush=True)
                    select.select([read_fd], [], [])
                finally:
                    signal.set_wakeup_fd(previous_wakeup_fd)
            finally:
                os.close(read_fd)
                os.close(write_fd)
    finally:
        (directory / 'calls.json').write_text(json.dumps(harness.calls), encoding='utf-8')


@pytest.mark.parametrize('failure', ['capture', 'thread'])
def test_bulk_host_report_survives_interrupted_sampler_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    primary: BaseException = (
        KeyboardInterrupt() if failure == 'capture' else RuntimeError('thread start failed')
    )
    captures = 0

    def capture(_sampler: host_sampling.HostSampler) -> HostSample:
        nonlocal captures
        captures += 1
        if failure == 'capture' and captures == 1:
            raise primary
        return HostSample(float(captures), float(captures), 20.0, 1.0, (), None)

    def start(_thread: threading.Thread) -> None:
        raise primary

    monkeypatch.setattr(host_sampling.HostSampler, 'capture', capture)
    if failure == 'thread':
        monkeypatch.setattr(threading.Thread, 'start', start)
    harness, calls = _bulk_host_harness(
        tmp_path,
        monkeypatch,
        real_sampler=True,
        expected_samples=1 if failure == 'capture' else 2,
    )

    with pytest.raises(BaseExceptionGroup) as caught, harness.context:
        pytest.fail('body must not run')

    assert calls[-1] == 'write-report'
    assert harness.report_path is not None
    serialized = json.loads(harness.report_path.read_text(encoding='utf-8'))
    assert len(serialized['load_metrics']['host_samples']) == (1 if failure == 'capture' else 2)
    if failure == 'capture':
        assert caught.value.exceptions == (primary,)
    else:
        assert _categories(caught.value) == ['setup']
        assert cast('SanitizedLiveError', caught.value.exceptions[0]).original is primary


@pytest.mark.parametrize('failure', ['none', 'setup', 'assertion', 'cleanup'])
def test_bulk_host_samples_survive_teardown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    harness, calls = _bulk_host_harness(tmp_path, monkeypatch)
    if failure == 'setup':
        harness.fail_at = 'create-shot'
    if failure == 'cleanup':
        harness.failures.add('close-source')

    def exercise() -> None:
        with harness.context:
            calls.append('launch')
            if failure == 'assertion':
                raise AssertionError('primary failure')

    if failure == 'none':
        exercise()
    else:
        with pytest.raises(BaseExceptionGroup) as caught:
            exercise()
        assert (
            _categories(caught.value)
            == {
                'setup': ['setup'],
                'assertion': ['test'],
                'cleanup': ['source-close'],
            }[failure]
        )
    assert calls.index('sampler-start') < calls.index('create-shot')
    assert (
        calls.index('sampler-stop') > calls.index('close-cleanup')
        if failure != 'setup'
        else (calls.index('sampler-stop') > calls.index('validate-child-import'))
    )
    assert calls[-1] == 'write-report'
    ordinary = live_conftest.LiveHooks(_settings(), tmp_path / 'ordinary')
    assert ordinary.host_sampler is None


def test_bulk_live_hooks_run_each_group_in_a_dedicated_worker(tmp_path: Path) -> None:
    site = cast('LiveSite', type('Site', (), {'project': {'id': 7}})())
    bulk_path, _ = live_conftest.LiveHooks(_settings(), tmp_path / 'bulk', bulk=True).write_config(
        site, 41
    )
    bulk_groups = tomllib.loads(bulk_path.read_text(encoding='utf-8'))['sites']['live']['groups']
    assert bulk_groups['bulk-eager']['worker'] == 'dedicated'
    assert bulk_groups['bulk-strict']['worker'] == 'dedicated'

    ordinary_path, _ = live_conftest.LiveHooks(_settings(), tmp_path / 'ordinary').write_config(
        site, 41
    )
    ordinary_groups = tomllib.loads(ordinary_path.read_text(encoding='utf-8'))['sites']['live'][
        'groups'
    ]
    assert all('worker' not in group for group in ordinary_groups.values())


def test_bulk_live_hooks_use_production_request_settings(tmp_path: Path) -> None:
    hooks = live_conftest.LiveHooks(_settings(), tmp_path, bulk=True)
    site = cast('LiveSite', type('Site', (), {'project': {'id': 7}})())
    path, _ = hooks.write_config(site, 41)
    credential = tomllib.loads(path.read_text(encoding='utf-8'))['sites']['live']['credentials'][
        'source'
    ]
    assert (
        not {'max_rpc_attempts', 'rpc_attempt_interval_secs', 'timeout_secs'} & credential.keys()
    )


def test_bulk_live_hooks_write_bulk_groups_and_recorder_environment(tmp_path: Path) -> None:

    hooks = live_conftest.LiveHooks(_settings(), tmp_path, bulk=True)
    site = cast('LiveSite', type('Site', (), {'project': {'id': 7}})())
    path, outputs = hooks.write_config(site, 41)
    config = tomllib.loads(path.read_text(encoding='utf-8'))['sites']['live']
    assert config['sources']['events']['late_event_window_secs'] == 30
    assert set(outputs) == {'bulk-eager', 'bulk-strict'}
    for name, group in config['groups'].items():
        assert group['execution'] == 'sync'
        assert group['ordering'] == name.removeprefix('bulk-')
        assert group['plugins'][0]['path'].endswith('/bulk_probe.py')
        assert group['plugins'][0]['config']['project_id'] == 7
        assert group['plugins'][0]['config']['output_path'] == str(outputs[name])
    assert hooks.environment['SG_EVENT_TEST_REQUEST_LOG'] == str(tmp_path / 'requests.jsonl')
    assert (
        hooks.environment['PYTHONPATH'].split(os.pathsep)[0].endswith('/fixtures/request_recorder')
    )


def test_context_cleans_up_partial_enter_failure(harness: _ContextHarness) -> None:
    harness.fail_at = 'create-shot'

    with (
        pytest.raises(BaseExceptionGroup, match='live acceptance setup') as caught,
        harness.context,
    ):
        pytest.fail('body must not run')

    assert _categories(caught.value) == ['setup']
    assert harness.calls[-4:] == [
        'close-source',
        'close-producer',
        'close-cleanup',
        'write-report',
    ]


def test_context_cleanup_is_idempotent(harness: _ContextHarness) -> None:
    with harness.context:
        pass

    assert harness.context.finalize() == ()
    assert harness.calls.count('stop-process') == 1
    assert harness.calls.count('write-report') == 1


def test_context_closes_open_clients_when_project_resolution_fails(
    harness: _ContextHarness,
) -> None:
    harness.fail_at = 'resolve-project'
    harness.failures.add('close-source')

    with (
        pytest.raises(BaseExceptionGroup, match='live acceptance setup') as caught,
        harness.context,
    ):
        pytest.fail('body must not run')

    assert _categories(caught.value) == ['setup', 'source-close']
    assert harness.calls[-4:] == [
        'close-source',
        'close-producer',
        'close-cleanup',
        'write-report',
    ]


def test_context_rejects_child_import_before_external_setup(harness: _ContextHarness) -> None:
    harness.fail_at = 'validate-child-import'

    with pytest.raises(BaseExceptionGroup, match='live acceptance setup'), harness.context:
        pytest.fail('body must not run')

    assert harness.calls == ['validate-provider', 'validate-child-import', 'write-report']


def test_context_report_uses_hostname_and_partial_observed_events(
    harness: _ContextHarness,
) -> None:
    report: LiveReport | None = None
    with harness.context as live_case:
        live_case.site.observed_events = (
            live_case.baseline_event,
            LiveEvent(id=101, phase_value='one', value=live_case.baseline_event.value),
        )
        report = live_case.report

    assert report is not None
    assert report.site_hostname == 'example.invalid'
    assert report.expected_ids == [101]
    assert report.source_visible_count == 1


def test_context_retains_body_and_every_finalizer_failure(harness: _ContextHarness) -> None:
    harness.failures.update(
        {
            'stop-process',
            'delete-shot',
            'verify-absent',
            'close-source',
            'close-producer',
            'close-cleanup',
        }
    )

    with pytest.raises(BaseExceptionGroup) as caught, harness.context:
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
    assert harness.calls[-1] == 'write-report'


def test_context_retains_cleanup_outcome_when_report_write_fails(
    harness: _ContextHarness,
) -> None:
    harness.fail_at = 'write-report'

    with pytest.raises(BaseExceptionGroup) as caught, harness.context:
        pass

    assert _categories(caught.value) == ['report']
    assert harness.report is not None
    assert harness.report.cleanup_outcome == ['shutdown:succeeded', 'cleanup:succeeded']
    assert harness.calls[-1] == 'write-report'


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

    def _validate_provider(_options: LiveOptions) -> object:
        return object()

    monkeypatch.setattr(live_conftest, 'open_provider_client', _open_client)
    monkeypatch.setattr(live_conftest, 'validate_child_import', _validate_import)
    monkeypatch.setattr(live_conftest, 'validate_live_provider', _validate_provider)
    hooks = _live_hooks(tmp_path)
    context = LiveCaseContext(settings=_settings(), directory=tmp_path, hooks=hooks)

    with pytest.raises(BaseExceptionGroup, match='live acceptance setup') as caught, context:
        pytest.fail('body must not run')

    assert _categories(caught.value) == ['setup', 'source-close', 'producer-close']
    assert cast('SanitizedLiveError', caught.value.exceptions[0]).original is open_failure
    assert cast('SanitizedLiveError', caught.value.exceptions[1]).original is source_close_failure
    assert (
        cast('SanitizedLiveError', caught.value.exceptions[2]).original is producer_close_failure
    )
    assert source.close_count == 1
    assert producer.close_count == 1


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

    def _validate_provider(_options: LiveOptions) -> object:
        return object()

    monkeypatch.setattr(live_conftest, 'open_provider_client', _open_client)
    monkeypatch.setattr(live_conftest, 'validate_child_import', _validate_import)
    monkeypatch.setattr(live_conftest, 'validate_live_provider', _validate_provider)
    hooks = _live_hooks(tmp_path)
    context = LiveCaseContext(settings=_settings(), directory=tmp_path, hooks=hooks)

    with pytest.raises(BaseExceptionGroup, match='live acceptance setup') as caught, context:
        pytest.fail('body must not run')

    assert caught.value.exceptions == (open_failure, close_failure)
    assert source.close_count == 1


def test_context_runs_successful_finalizers_in_order(harness: _ContextHarness) -> None:
    with harness.context:
        harness.calls.append('body')

    assert harness.calls == [
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
    harness.failures.add('delete-shot')

    with pytest.raises(BaseExceptionGroup) as caught, harness.context:
        pass

    assert _categories(caught.value) == ['delete']
    assert harness.report_path is not None
    payload = json.loads(harness.report_path.read_text(encoding='utf-8'))
    assert payload['cleanup_outcome'] == ['shutdown:succeeded', 'cleanup:failed']
    assert payload['failure_categories'] == ['delete']


def test_context_serializes_successful_cleanup_outcome(harness: _ContextHarness) -> None:
    with harness.context:
        pass

    assert harness.report_path is not None
    payload = json.loads(harness.report_path.read_text(encoding='utf-8'))
    assert payload['cleanup_outcome'] == ['shutdown:succeeded', 'cleanup:succeeded']


def test_interrupt_after_shot_creation_cleans_up(tmp_path: Path) -> None:
    script = (
        'import sys; from pathlib import Path; '
        f'from tests.live.test_context import {_interrupt_child.__name__}; '
        f'{_interrupt_child.__name__}(Path(sys.argv[1]))'
    )
    with subprocess.Popen(
        [sys.executable, '-c', script, str(tmp_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as child:
        try:
            assert child.stdout is not None
            assert select.select([child.stdout], [], [])[0], 'child did not create the Shot'
            assert child.stdout.readline().strip() == 'shot-created'
            child.send_signal(signal.SIGINT)
            _, stderr = child.communicate()
            assert child.returncode != 0
            assert 'KeyboardInterrupt' in stderr
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate()
    calls = json.loads((tmp_path / 'calls.json').read_text(encoding='utf-8'))
    assert 'create-shot' in calls
    assert calls[-7:] == [
        'stop-process',
        'delete-shot',
        'verify-absent',
        'close-source',
        'close-producer',
        'close-cleanup',
        'write-report',
    ]
    reports = list((tmp_path / 'reports').glob('*.json'))
    assert len(reports) == 1
    report = json.loads(reports[0].read_text(encoding='utf-8'))
    assert report['cleanup_outcome'] == ['shutdown:succeeded', 'cleanup:succeeded']


def test_live_cleanup_keeps_uncategorized_bulk_failure_as_cleanup(tmp_path: Path) -> None:
    hooks = live_conftest.LiveHooks(_settings(), tmp_path)
    failure = RuntimeError('bulk delete failed')
    site = cast('LiveSite', SimpleNamespace(cleanup=lambda: (failure,)))
    assert hooks.cleanup(site) == (('cleanup', failure),)


def test_live_failure_and_report_accept_single_pass_values(tmp_path: Path) -> None:
    failure = SanitizedLiveError(
        'setup',
        RuntimeError('secret value'),
        (secret for secret in ('secret',)),
    )
    report = LiveReport(
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


@pytest.mark.parametrize(
    ('bulk', 'mode'), [(False, 'sync'), (False, 'sync+async'), (True, 'sync')]
)
def test_live_hooks_allow_configured_worker_shutdown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bulk: bool, mode: str
) -> None:
    settings = _settings()
    settings = replace(settings, options=replace(settings.options, mode=cast('LiveMode', mode)))
    hooks = live_conftest.LiveHooks(settings, tmp_path, bulk=bulk)
    site = cast('LiveSite', SimpleNamespace(project={'id': 7}))
    path, _ = hooks.write_config(site, 41)
    deadline = tomllib.loads(path.read_text(encoding='utf-8'))['daemon']['shutdown_deadline_secs']
    monkeypatch.setenv('SG_EVENT_TEST_SOURCE_KEY', 'source-key')
    workers = len(build_worker_specs(load_config(path).sites['live']))
    assert workers == (3 if bulk else 1)
    assert hooks.build_process(path).timeout_secs >= workers * 2 * deadline


@pytest.mark.parametrize('control', [KeyboardInterrupt(), asyncio.CancelledError()])
def test_context_preserves_process_control_failure_identity(
    harness: _ContextHarness,
    control: BaseException,
) -> None:
    harness.failures.add('close-source')

    with pytest.raises(BaseExceptionGroup) as caught, harness.context:
        raise control

    assert caught.value.exceptions[0] is control
    assert isinstance(caught.value.exceptions[1], SanitizedLiveError)
    assert harness.calls[-1] == 'write-report'


@pytest.mark.parametrize('mode', ['sync', 'sync+async'])
def test_ordinary_live_hooks_configure_eager_probe_groups(tmp_path: Path, mode: str) -> None:

    settings = _settings()
    settings = replace(settings, options=replace(settings.options, mode=cast('LiveMode', mode)))
    hooks = live_conftest.LiveHooks(settings, tmp_path)
    path, outputs = hooks.write_config(cast('LiveSite', None), 41)
    config = tomllib.loads(path.read_text(encoding='utf-8'))['sites']['live']
    assert set(outputs) == ({'sync'} if mode == 'sync' else {'sync', 'async'})
    assert 'late_event_window_secs' not in config['sources']['events']
    for name, group in config['groups'].items():
        assert group['execution'] == name
        assert group['ordering'] == 'eager'
        assert group['plugins'][0]['path'].endswith(f'/{name}_probe.py')


def test_ordinary_live_hooks_ignore_inherited_request_recorder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inherited = tmp_path / 'another-case.jsonl'
    monkeypatch.setenv('SG_EVENT_TEST_REQUEST_LOG', str(inherited))

    ordinary = live_conftest.LiveHooks(_settings(), tmp_path / 'ordinary')
    bulk = live_conftest.LiveHooks(_settings(), tmp_path / 'bulk', bulk=True)

    assert 'SG_EVENT_TEST_REQUEST_LOG' not in ordinary.environment
    assert bulk.environment['SG_EVENT_TEST_REQUEST_LOG'] == str(tmp_path / 'bulk/requests.jsonl')


def test_ordinary_live_hooks_keep_short_request_settings(tmp_path: Path) -> None:
    hooks = live_conftest.LiveHooks(_settings(), tmp_path)
    path, _ = hooks.write_config(cast('LiveSite', object()), 41)
    credential = tomllib.loads(path.read_text(encoding='utf-8'))['sites']['live']['credentials'][
        'source'
    ]
    assert credential['max_rpc_attempts'] == 1
    assert credential['rpc_attempt_interval_secs'] == 0.1
    assert credential['timeout_secs'] == 5.0
