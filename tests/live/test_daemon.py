from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Literal, cast

import pytest

import tests.live._daemon as daemon_module
from sg_event_daemon._config import load_config
from sg_event_daemon._events import Event
from sg_event_daemon.testing import (
    HarnessSettings,
    drive_async_plugin,
    drive_sync_plugin,
    load_plugin,
)
from tests._live_mode import LiveOptions
from tests._stall_policy import STALL_DEADLINE_SECS
from tests.live._daemon import (
    LiveCase,
    LiveReport,
    ProbeRecord,
    SanitizedLiveError,
    SupervisorProcess,
    assert_group_cursors,
    assert_group_deliveries,
    build_child_environment,
    read_probe_records,
    validate_child_import,
    wait_for_group_event_set,
    wait_for_group_records,
    write_daemon_config,
    write_report,
)
from tests.live._site import Credential, LiveEvent, LiveSettings, LiveSite

if TYPE_CHECKING:
    from collections.abc import Mapping

_FIXTURES = Path(__file__).with_name('fixtures')


def _event(
    event_id: int,
    *,
    attribute_name: str = 'description',
    phase_value: str | None = 'phase-one',
    shot_id: int = 41,
) -> dict[str, object]:
    metadata: dict[str, object] = {'attribute_name': attribute_name}
    if phase_value is not None:
        metadata['new_value'] = phase_value
    return {
        'attribute_name': attribute_name,
        'created_at': '2026-08-10T12:00:00Z',
        'entity': {'id': shot_id, 'name': 'acceptance-shot', 'type': 'Shot'},
        'event_type': 'Shotgun_Shot_Change',
        'id': event_id,
        'meta': metadata,
        'project': {'id': 7, 'name': 'Live Acceptance', 'type': 'Project'},
        'session_uuid': None,
        'type': 'EventLogEntry',
        'user': None,
    }


def _live_event(event_id: int, phase_value: str) -> LiveEvent:
    return LiveEvent(
        id=event_id,
        phase_value=phase_value,
        value=Event(_event(event_id, phase_value=phase_value)),
    )


def _paths(tmp_path: Path) -> dict[str, Path]:
    return {
        'async': tmp_path / 'async.jsonl',
        'config': tmp_path / 'daemon.toml',
        'state': tmp_path / 'state',
        'sync': tmp_path / 'sync.jsonl',
    }


def _settings(*, mode: Literal['sync', 'sync+async'] = 'sync') -> LiveSettings:
    options = LiveOptions(
        provider='sg-python-api',
        mode=mode,
        load_events=1_000,
        report_dir=None,
        run_live=True,
        run_live_load=False,
    )
    return LiveSettings(
        site='https://example.invalid/path?private=discarded',
        project='Live Acceptance "Quoted"',
        source=Credential(script_name='source "reader"', api_key='source-key'),
        producer=Credential(script_name='producer', api_key='producer-key'),
        cleanup=Credential(script_name='cleanup', api_key='cleanup-key'),
        options=options,
    )


def _live_case(tmp_path: Path, process: SupervisorProcess) -> LiveCase:
    baseline = LiveEvent(id=100, phase_value='baseline', value=Event(_event(100)))
    return LiveCase(
        settings=_settings(mode='sync+async'),
        site=cast('LiveSite', object()),
        process=process,
        config_path=tmp_path / 'config.toml',
        baseline_event=baseline,
        output_paths={
            'async': tmp_path / 'async.jsonl',
            'sync': tmp_path / 'sync.jsonl',
        },
        report=LiveReport(
            site_hostname='example.invalid',
            project_name='Live Acceptance',
            provider='sg-python-api',
            mode='sync+async',
            namespace='acceptance-run',
        ),
    )


def _status(
    *,
    conditions: tuple[str, ...],
    cursor: int = 104,
    failure_kind: str | None = None,
    group_status: str = 'running',
    primary_state: str,
) -> dict[str, object]:
    return {
        'conditions': conditions,
        'groups': (
            {
                'conditions': conditions,
                'cursor': cursor,
                'failure_kind': failure_kind,
                'group': 'sync',
                'parked': group_status == 'parked',
                'site': 'live',
                'status': group_status,
            },
        ),
        'liveness': True,
        'primary_state': primary_state,
        'readiness': True,
    }


def _script_graceful_reap_expiry(
    process: SupervisorProcess,
    monkeypatch: pytest.MonkeyPatch,
    *,
    group_signals: list[tuple[int, int]] | None = None,
    require_direct_kill: bool = False,
) -> list[str]:
    phases: list[str] = []
    direct_kills: list[int] = []
    if require_direct_kill:
        child = process.process
        assert child is not None
        original_kill = child.kill

        def kill() -> None:
            direct_kills.append(child.pid)
            original_kill()

        monkeypatch.setattr(child, 'kill', kill)

    def reap(child: subprocess.Popen[bytes]) -> bool:
        if not phases:
            assert child.poll() is None
            phases.append('graceful-expired')
            return False
        assert phases == ['graceful-expired']
        if group_signals is not None:
            assert group_signals == [(child.pid, signal.SIGKILL)]
        if require_direct_kill:
            assert direct_kills == [child.pid]
        child.wait()
        phases.append('forced-reaped')
        return True

    monkeypatch.setattr(process, '_reap_with_timeout', reap)
    return phases


def _wait_child_ready_log(process: SupervisorProcess) -> None:
    while 'ready' not in process.log_paths[0].read_text(encoding='utf-8'):
        child = process.process
        assert child is not None
        assert child.poll() is None, 'child exited before ready'
        time.sleep(0.01)


def _write_probe_rows(path: Path, group: str, rows: list[tuple[int, str]]) -> None:
    payloads = [
        {
            'attribute_name': 'description',
            'delivery_attempt': 1,
            'entity_id': 41,
            'entity_type': 'Shot',
            'event_id': event_id,
            'event_type': 'Shotgun_Shot_Change',
            'group': group,
            'is_operator_replay': False,
            'phase_value': phase_value,
        }
        for event_id, phase_value in rows
    ]
    path.write_text(
        ''.join(json.dumps(payload) + '\n' for payload in payloads),
        encoding='utf-8',
    )


@pytest.fixture
def settings() -> LiveSettings:
    return _settings()


def test_child_environment_imports_checkout_from_unrelated_directory(
    settings: LiveSettings,
    tmp_path: Path,
) -> None:
    source_root = (Path(__file__).resolve().parents[2] / 'src').resolve()
    ambient = {
        'PYTHONPATH': '/existing/path',
        'SG_EVENT_TEST_PRODUCER_KEY': 'producer-key',
        'SG_EVENT_TEST_CLEANUP_KEY': 'cleanup-key',
    }

    environment = build_child_environment(settings, ambient, source_root)

    assert environment['PYTHONPATH'].split(os.pathsep) == [
        os.fspath(source_root),
        '/existing/path',
    ]
    assert environment['SG_EVENT_TEST_SOURCE_KEY'] == 'source-key'
    assert 'SG_EVENT_TEST_PRODUCER_KEY' not in environment
    assert 'SG_EVENT_TEST_CLEANUP_KEY' not in environment
    validate_child_import(environment, tmp_path)


def test_child_import_reports_subprocess_failure(
    monkeypatch: pytest.MonkeyPatch,
    settings: LiveSettings,
    tmp_path: Path,
) -> None:
    environment = build_child_environment(settings, {}, tmp_path / 'src')

    def fail_import(
        command: tuple[str, ...],
        **_options: object,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, '', 'module unavailable')

    monkeypatch.setattr(daemon_module.subprocess, 'run', fail_import)

    with pytest.raises(
        RuntimeError,
        match=r'child import validation failed \(1\): module unavailable',
    ) as caught:
        validate_child_import(environment, tmp_path)

    assert 'source-key' not in str(caught.value)


def test_daemon_config_round_trips_both_modes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    monkeypatch.setenv('SG_EVENT_TEST_SOURCE_KEY', 'test-source-key')

    sync = write_daemon_config(paths, _settings(mode='sync'), shot_id=41)
    sync_config = load_config(sync)
    assert tuple(sync_config.sites['live'].groups) == ('sync',)

    both = write_daemon_config(paths, _settings(mode='sync+async'), shot_id=41)
    both_config = load_config(both)
    source_credential = both_config.sites['live'].credentials['source']
    assert tuple(both_config.sites['live'].groups) == ('async', 'sync')
    assert source_credential.max_rpc_attempts == 1
    assert source_credential.rpc_attempt_interval_secs == 0.1
    assert source_credential.timeout_secs == 5.0
    assert both_config.sites['live'].groups['async'].execution == 'async'
    assert both_config.sites['live'].groups['sync'].execution == 'sync'
    text = both.read_text(encoding='utf-8')
    assert 'source-key' not in text
    assert 'SG_EVENT_TEST_SOURCE_KEY' in text
    assert paths['state'].stat().st_mode & 0o777 == 0o700
    assert both_config.daemon.health_interval_secs == 2.0
    assert both_config.daemon.supervisor_poll_interval_secs > 0
    assert both_config.sites['live'].sources['events'].fetch_interval_secs > 0
    for group_name, group in both_config.sites['live'].groups.items():
        plugin = group.plugins[0]
        assert plugin.path.is_absolute()
        assert plugin.config == {
            'output_path': os.fspath(paths[group_name].resolve()),
            'shot_id': 41,
        }
        assert group.callback_credentials == 'source'
        assert group.ordering == 'eager'


def test_delivery_wait_reads_only_new_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / 'probe.jsonl'
    _write_probe_rows(path, 'sync', [(101, 'first'), (102, 'second')])
    first, second = path.read_text().splitlines(keepends=True)
    second = second.rstrip('\n')
    path.write_text(first)
    case = cast(
        'daemon_module.LiveCase', SimpleNamespace(output_paths={'sync': path}, probe_readers={})
    )
    parsed: list[str] = []
    original = daemon_module.json.loads

    def loads(line: str) -> object:
        parsed.append(line)
        return original(line)

    monkeypatch.setattr(daemon_module.json, 'loads', loads)
    assert len(daemon_module.read_all_groups(case)['sync']) == 1
    assert len(daemon_module.read_all_groups(case)['sync']) == 1
    assert len(parsed) == 1
    with path.open('a') as stream:
        stream.write(second)
    assert len(daemon_module.read_all_groups(case)['sync']) == 1
    with path.open('a') as stream:
        stream.write('\n')
    assert [row.event_id for row in daemon_module.read_all_groups(case)['sync']] == [101, 102]
    assert len(parsed) == 2
    with path.open('a') as stream:
        stream.write('bad JSON\n')
    with pytest.raises(ValueError, match='row 3 is invalid JSON'):
        daemon_module.read_all_groups(case)


@pytest.mark.parametrize(
    ('primary_state', 'conditions', 'group_status'),
    [
        ('healthy', ('healthy',), 'running'),
        ('idle', ('idle',), 'running'),
        ('parked', ('parked',), 'parked'),
    ],
)
def test_group_cursors_accept_production_ready_statuses(
    primary_state: str,
    conditions: tuple[str, ...],
    group_status: str,
) -> None:
    assert_group_cursors(
        _status(
            conditions=conditions,
            group_status=group_status,
            primary_state=primary_state,
        ),
        expected_groups=('sync',),
        expected_floor=103,
    )


def test_group_cursors_reject_duplicate_group_identity() -> None:
    status = _status(conditions=('healthy',), primary_state='healthy')
    group = cast('tuple[dict[str, object], ...]', status['groups'])[0]
    status['groups'] = (group, dict(group))

    with pytest.raises(AssertionError, match='unique'):
        assert_group_cursors(status, expected_groups=('sync',), expected_floor=103)


@pytest.mark.parametrize(
    ('status', 'message'),
    [
        (
            _status(
                conditions=('healthy',),
                failure_kind='callback',
                group_status='callback-failed',
                primary_state='healthy',
            ),
            'final group status retains a fatal condition',
        ),
        (
            _status(conditions=('healthy',), cursor=102, primary_state='healthy'),
            'final group cursor is below the expected floor',
        ),
        (
            _status(conditions=('healthy',), failure_kind='callback', primary_state='healthy'),
            'final group status retains a fatal failure',
        ),
    ],
)
def test_group_cursors_reject_fatal_or_insufficient_group_progress(
    status: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(AssertionError, match=message):
        assert_group_cursors(status, expected_groups=('sync',), expected_floor=103)


def test_group_cursors_reject_fatal_supervisor_status() -> None:
    status = _status(conditions=('callback-failed',), primary_state='callback-failed')

    with pytest.raises(AssertionError, match='readiness-compatible'):
        assert_group_cursors(status, expected_groups=('sync',), expected_floor=103)


def test_group_cursors_reject_malformed_group_identity() -> None:
    status = _status(conditions=('healthy',), primary_state='healthy')
    group = dict(cast('tuple[dict[str, object], ...]', status['groups'])[0])
    group['group'] = 17
    status['groups'] = (group,)

    with pytest.raises(TypeError, match='identity'):
        assert_group_cursors(status, expected_groups=('sync',), expected_floor=103)


def test_group_cursors_reject_omitted_configured_group() -> None:
    status = _status(conditions=('healthy',), primary_state='healthy')

    with pytest.raises(AssertionError, match='identity coverage'):
        assert_group_cursors(
            status,
            expected_groups=('async', 'sync'),
            expected_floor=103,
        )


@pytest.mark.parametrize('consumer', ['readiness', 'delivery-report'])
def test_live_acceptance_consumes_detailed_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    consumer: str,
) -> None:
    detailed = _status(conditions=('idle',), cursor=101, primary_state='idle')
    compact = {
        'complete': True,
        'conditions': ['idle'],
        'generated_at': 1.0,
        'liveness': True,
        'readiness': True,
        'snapshot_age_seconds': 0.0,
    }

    def _run(
        command: tuple[str, ...] | list[str],
        **_options: object,
    ) -> subprocess.CompletedProcess[str]:
        payload = compact if '--check' in command else detailed
        return subprocess.CompletedProcess(command, 0, json.dumps(payload), '')

    monkeypatch.setattr(daemon_module.subprocess, 'run', _run)
    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml',
        environment={},
        directory=tmp_path,
        timeout_secs=0.2,
    )
    monkeypatch.setattr(process, 'process', SimpleNamespace(poll=lambda: None))

    if consumer == 'readiness':
        status = process.wait_ready()
    else:
        live_case = _live_case(tmp_path, process)
        live_case.output_paths = {'sync': tmp_path / 'sync.jsonl'}
        _write_probe_rows(live_case.output_paths['sync'], 'sync', [(101, 'one')])
        assert_group_deliveries(
            live_case,
            (_live_event(101, 'one'),),
            excluded_ids=(100,),
        )
        status = live_case.report.status_summary

    assert_group_cursors(status, expected_groups=('sync',), expected_floor=101)


def test_live_stop_reports_log_close_failure_during_test_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    close_error = OSError('log close failed')

    class Handle:
        closed = False

        def close(self) -> None:
            self.closed = True
            raise close_error

    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml', directory=tmp_path, environment={}, timeout_secs=1.0
    )
    vars(process)['process'] = object()
    vars(process)['pgid'] = 123
    vars(process)['stdout'] = Handle()

    def terminate(*_args: object, **_kwargs: object) -> tuple[BaseException, ...]:
        return ()

    def fail() -> None:
        raise RuntimeError('test failed')

    monkeypatch.setattr(SupervisorProcess, 'terminate_and_reap', terminate)

    ambient_error: RuntimeError | None = None
    caught_error: OSError | None = None
    try:
        fail()
    except RuntimeError as test_error:
        with pytest.raises(OSError, match='log close failed') as raised:
            process.stop()
        ambient_error = test_error
        caught_error = raised.value

    assert ambient_error is not None
    assert caught_error is not None
    assert not hasattr(ambient_error, '__notes__')
    assert caught_error is close_error
    assert vars(process)['pgid'] is None


def test_nonzero_status_has_bounded_log_tails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _run(
        command: tuple[str, ...] | list[str],
        **_options: object,
    ) -> subprocess.CompletedProcess[str]:
        text = 'x' * 100_000
        return subprocess.CompletedProcess(command, 2, text, text)

    monkeypatch.setattr(daemon_module.subprocess, 'run', _run)
    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml',
        environment={},
        directory=tmp_path,
        timeout_secs=0.2,
    )

    with pytest.raises(RuntimeError) as caught:
        process.status(check='readiness')

    assert len(str(caught.value)) < 20_000


def test_ordered_probe_timeout_retains_bounded_sanitized_diagnostics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    credential_value = 'source-key'
    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml',
        environment={'SG_EVENT_TEST_SOURCE_KEY': credential_value},
        directory=tmp_path,
        timeout_secs=0.2,
    )

    def _run(
        command: tuple[str, ...] | list[str],
        **_options: object,
    ) -> subprocess.CompletedProcess[str]:
        status = {'liveness': True, 'message': credential_value, 'readiness': False}
        return subprocess.CompletedProcess(command, 1, json.dumps(status), '')

    monkeypatch.setattr(daemon_module.subprocess, 'run', _run)
    process.status(check='readiness')
    process.log_paths[0].write_text('o' * 100_000 + credential_value, encoding='utf-8')
    process.log_paths[1].write_text('e' * 100_000 + credential_value, encoding='utf-8')
    live_case = _live_case(tmp_path, process)
    _write_probe_rows(live_case.output_paths['sync'], 'sync', [(101, 'one')])
    expected = (_live_event(101, 'one'), _live_event(102, 'two'))
    monkeypatch.setattr(daemon_module, 'LIVE_TIMEOUT_SECS', 0.0)

    with pytest.raises(AssertionError, match='ordered events') as caught:
        wait_for_group_records(live_case, expected_events=expected)

    diagnostic = json.loads(str(caught.value).partition('diagnostics=')[2])
    assert credential_value not in str(caught.value)
    assert len(str(caught.value)) < 20_000
    assert diagnostic['expected_event_ids'] == [101, 102]
    assert diagnostic['last_status'] == {
        'liveness': True,
        'message': '<redacted>',
        'readiness': False,
    }
    assert diagnostic['last_group_callback_rows']['sync'][0]['event_id'] == 101
    assert diagnostic['daemon_stdout_tail'].endswith('<redacted>')
    assert diagnostic['daemon_stderr_tail'].endswith('<redacted>')
    assert len(diagnostic['daemon_stdout_tail']) == 8_192
    assert len(diagnostic['daemon_stderr_tail']) == 8_192


@pytest.mark.parametrize('execution', ['sync', 'async'])
def test_probe_plugins_write_only_matching_rows_with_shared_schema(
    execution: Literal['sync', 'async'],
    tmp_path: Path,
) -> None:
    output = tmp_path / f'{execution}.jsonl'
    loaded = load_plugin(
        _FIXTURES / f'{execution}_probe.py',
        plugin_id=f'{execution}-probe',
        execution=execution,
        settings=HarnessSettings(
            group=execution,
            config={'output_path': os.fspath(output), 'shot_id': 41},
        ),
    )
    first = (
        _event(101, shot_id=42),
        _event(102, attribute_name='sg_status_list'),
        _event(103),
    )
    second = (_event(104, phase_value=None),)

    if execution == 'sync':
        assert drive_sync_plugin(loaded, first, delivery_attempt=3, is_operator_replay=True).ok
        assert drive_sync_plugin(loaded, second).ok
    else:
        assert asyncio.run(
            drive_async_plugin(loaded, first, delivery_attempt=3, is_operator_replay=True)
        ).ok
        assert asyncio.run(drive_async_plugin(loaded, second)).ok

    assert read_probe_records(output, expected_group=execution) == (
        ProbeRecord(
            event_id=103,
            event_type='Shotgun_Shot_Change',
            attribute_name='description',
            entity_type='Shot',
            entity_id=41,
            delivery_attempt=3,
            is_operator_replay=True,
            group=execution,
            phase_value='phase-one',
        ),
        ProbeRecord(
            event_id=104,
            event_type='Shotgun_Shot_Change',
            attribute_name='description',
            entity_type='Shot',
            entity_id=41,
            delivery_attempt=1,
            is_operator_replay=False,
            group=execution,
            phase_value=None,
        ),
    )


@pytest.mark.parametrize(
    'row',
    [
        {'event_id': 1},
        {
            'attribute_name': 'description',
            'delivery_attempt': 1,
            'entity_id': True,
            'entity_type': 'Shot',
            'event_id': 1,
            'event_type': 'Shotgun_Shot_Change',
            'group': 'sync',
            'is_operator_replay': False,
            'phase_value': None,
        },
    ],
)
def test_probe_reader_rejects_invalid_rows(tmp_path: Path, row: dict[str, object]) -> None:
    output = tmp_path / 'records.jsonl'
    output.write_text(json.dumps(row) + '\n', encoding='utf-8')

    with pytest.raises(ValueError, match='probe'):
        read_probe_records(output, expected_group='sync')


def test_process_close_logs_attempts_both_handles_and_resets_fields(tmp_path: Path) -> None:
    attempts: list[str] = []
    failure = OSError('stdout close failed')

    class Handle:
        closed = False

        def __init__(self, name: str) -> None:
            self._name = name

        def close(self) -> None:
            attempts.append(self._name)
            if self._name == 'stdout':
                raise failure

    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml', directory=tmp_path, environment={}, timeout_secs=1.0
    )
    vars(process)['stdout'] = Handle('stdout')
    vars(process)['stderr'] = Handle('stderr')
    with pytest.raises(OSError, match='fail') as caught:
        process.close_logs()
    assert caught.value is failure
    assert attempts == ['stdout', 'stderr']
    assert vars(process)['stdout'] is None
    assert vars(process)['stderr'] is None


def test_process_stop_preserves_termination_error_when_log_cleanup_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml', directory=tmp_path, environment={}, timeout_secs=1.0
    )
    vars(process)['process'] = object()
    vars(process)['pgid'] = 123
    initiating = RuntimeError('termination failed')

    def terminate(*_args: object, **_kwargs: object) -> None:
        raise initiating

    def close_logs(*_args: object) -> None:
        raise OSError('log cleanup failed')

    monkeypatch.setattr(SupervisorProcess, 'terminate_and_reap', terminate)
    monkeypatch.setattr(SupervisorProcess, 'close_logs', close_logs)
    with pytest.raises(RuntimeError, match='termination failed') as caught:
        process.stop()
    assert caught.value is initiating
    assert vars(process)['pgid'] is None
    assert any('log cleanup failed' in note for note in initiating.__notes__)


def test_report_is_sanitized_optional_and_timestamped(tmp_path: Path) -> None:
    report = LiveReport(
        site_hostname='example.invalid',
        project_name='Live Acceptance',
        provider='sg-python-api',
        mode='sync+async',
        namespace='acceptance-run',
        expected_ids=[101, 102],
        source_visible_count=2,
        requested_updates=3,
        successful_updates=2,
    )
    secrets = ('source-key', 'producer-key', 'cleanup-key')
    error = RuntimeError('trace' * 4_000 + ' source-key producer-key cleanup-key')
    failure = SanitizedLiveError('provider', error, secrets)
    terminal = StringIO()

    path = write_report(report, tmp_path, failures=(failure,), terminal=terminal)

    assert path is not None
    assert path.parent == tmp_path.resolve()
    assert list(tmp_path.glob('*.json')) == [path]
    rendered = path.read_text(encoding='utf-8')
    summary = terminal.getvalue()
    for secret in secrets:
        assert secret not in str(failure)
        assert secret not in failure.traceback_text
        assert secret not in rendered
        assert secret not in summary
    assert str(failure).count('<redacted>') >= len(secrets)
    assert failure.traceback_text.count('<redacted>') >= len(secrets)
    assert failure.original is error
    assert len(failure.traceback_text) < 8_000
    assert 'sync+async' in summary
    assert 'expected=2' in summary
    assert write_report(report, None, terminal=StringIO()) is None


def test_report_schema_excludes_raw_responses_and_daemon_logs(tmp_path: Path) -> None:
    report = LiveReport(
        site_hostname='example.invalid',
        project_name='Live Acceptance',
        provider='shotgun-api3',
        mode='sync',
        namespace='acceptance-run',
    )
    report.status_summary = {'liveness': True, 'readiness': True}
    path = write_report(report, tmp_path, terminal=StringIO())
    assert path is not None
    payload = cast('Mapping[str, object]', json.loads(path.read_text(encoding='utf-8')))

    assert set(payload) == set(report.__dataclass_fields__)
    assert 'response' not in payload
    assert 'stdout' not in payload
    assert 'stderr' not in payload


def test_supervisor_kills_only_stuck_child_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    command_source = (
        'import signal,threading; signal.signal(signal.SIGTERM, signal.SIG_IGN); '
        "print('ready', flush=True); threading.Event().wait()"
    )
    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml',
        environment={},
        directory=tmp_path,
        timeout_secs=STALL_DEADLINE_SECS,
        command=(
            sys.executable,
            '-c',
            command_source,
        ),
    )
    process.start()
    child = cast('subprocess.Popen[bytes]', vars(process)['process'])
    try:
        _wait_child_ready_log(process)
        group_signals: list[tuple[int, int]] = []
        phases = _script_graceful_reap_expiry(process, monkeypatch, group_signals=group_signals)
        pid = process.pid
        original_killpg = os.killpg

        def killpg(pgid: int, signum: int) -> None:
            group_signals.append((pgid, signum))
            original_killpg(pgid, signum)

        monkeypatch.setattr(daemon_module.os, 'killpg', killpg)

        failures = process.stop()
    finally:
        unreaped = child.poll() is None
        if unreaped:
            child.kill()
            child.wait()

    assert not unreaped
    assert pid is not None
    assert pid > 1
    assert group_signals == [(pid, signal.SIGKILL)]
    assert phases == ['graceful-expired', 'forced-reaped']
    assert len(failures) == 1
    assert isinstance(failures[0], SanitizedLiveError)
    assert failures[0].category == 'shutdown'
    assert process.stop() == failures


def test_supervisor_reaps_child_when_validated_group_kill_races(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_source = (
        'import signal,threading; signal.signal(signal.SIGTERM, signal.SIG_IGN); '
        "print('ready', flush=True); threading.Event().wait()"
    )
    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml',
        environment={},
        directory=tmp_path,
        timeout_secs=STALL_DEADLINE_SECS,
        command=(sys.executable, '-c', command_source),
    )
    process.start()
    child = cast('subprocess.Popen[bytes]', vars(process)['process'])
    try:
        _wait_child_ready_log(process)
        phases = _script_graceful_reap_expiry(process, monkeypatch, require_direct_kill=True)
        race = ProcessLookupError('test-owned group disappeared')

        def _killpg(_pgid: int, _signal: int) -> None:
            raise race

        monkeypatch.setattr(daemon_module.os, 'killpg', _killpg)
        failures = process.stop()
    finally:
        unreaped = child.poll() is None
        if unreaped:
            child.kill()
            child.wait()

    assert not unreaped
    assert phases == ['graceful-expired', 'forced-reaped']
    assert any(
        isinstance(failure, SanitizedLiveError) and failure.original is race
        for failure in failures
    )


def test_supervisor_reaps_sigterm_ignoring_child_when_group_is_unsafe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    command_source = (
        'import signal,threading; signal.signal(signal.SIGTERM, signal.SIG_IGN); '
        "print('ready', flush=True); threading.Event().wait()"
    )
    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml',
        environment={},
        directory=tmp_path,
        timeout_secs=STALL_DEADLINE_SECS,
        command=(sys.executable, '-c', command_source),
    )
    group_signals: list[int] = []
    phases: list[str] = []

    def _getpgid(_pid: int) -> int:
        nonlocal phases
        _wait_child_ready_log(process)
        phases = _script_graceful_reap_expiry(process, monkeypatch, require_direct_kill=True)
        return 0

    def _killpg(pgid: int, _signal: int) -> None:
        group_signals.append(pgid)

    monkeypatch.setattr(daemon_module.os, 'getpgid', _getpgid)
    monkeypatch.setattr(daemon_module.os, 'killpg', _killpg)

    child: subprocess.Popen[bytes] | None = None
    try:
        with pytest.raises(RuntimeError, match='unsafe process group'):
            process.start()
    finally:
        child = cast('subprocess.Popen[bytes] | None', vars(process)['process'])
        if child is not None and child.poll() is None:
            unreaped = True
            child.kill()
            child.wait()
        else:
            unreaped = False
    assert not unreaped
    assert group_signals == []
    assert phases == ['graceful-expired', 'forced-reaped']


def test_supervisor_reaps_unresolved_process_without_group_signal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    killed: list[int] = []

    def _getpgid(_pid: int) -> int:
        raise ProcessLookupError

    def _killpg(pgid: int, _signal: int) -> None:
        killed.append(pgid)

    monkeypatch.setattr(daemon_module.os, 'getpgid', _getpgid)
    monkeypatch.setattr(daemon_module.os, 'killpg', _killpg)
    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml',
        environment={},
        directory=tmp_path,
        timeout_secs=0.2,
        command=(sys.executable, '-c', 'import threading; threading.Event().wait()'),
    )

    try:
        with pytest.raises(RuntimeError, match='could not be resolved'):
            process.start()
    finally:
        child = cast('subprocess.Popen[bytes] | None', vars(process)['process'])
        if child is not None and child.poll() is None:
            child.kill()
            child.wait()
        process.close_logs()

    assert killed == []


def test_supervisor_rejects_empty_injected_command(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match='command'):
        SupervisorProcess(
            config_path=tmp_path / 'config.toml',
            environment={},
            directory=tmp_path,
            timeout_secs=0.2,
            command=(),
        )


@pytest.mark.parametrize('unsafe_pgid', [0, 1, os.getpgrp()])
def test_supervisor_rejects_unsafe_process_groups_before_use(
    unsafe_pgid: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    killed: list[int] = []

    def _getpgid(_pid: int) -> int:
        return unsafe_pgid

    def _killpg(pgid: int, _signal: int) -> None:
        killed.append(pgid)

    monkeypatch.setattr(daemon_module.os, 'getpgid', _getpgid)
    monkeypatch.setattr(daemon_module.os, 'killpg', _killpg)
    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml',
        environment={},
        directory=tmp_path,
        timeout_secs=0.2,
        command=(sys.executable, '-c', 'import threading; threading.Event().wait()'),
    )

    try:
        with pytest.raises(RuntimeError, match='unsafe process group'):
            process.start()
    finally:
        child = cast('subprocess.Popen[bytes] | None', vars(process)['process'])
        if child is not None and child.poll() is None:
            child.kill()
            child.wait()
        process.close_logs()

    assert killed == []


def test_supervisor_status_parses_json_and_retains_last_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []

    def _run(
        command: tuple[str, ...] | list[str],
        **_options: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(command))
        payload: dict[str, object] = {
            'conditions': [],
            'detail': 'source-key',
            'groups': [],
            'liveness': True,
            'readiness': True,
        }
        return subprocess.CompletedProcess(command, 0, json.dumps(payload), '')

    monkeypatch.setattr(daemon_module.subprocess, 'run', _run)
    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml',
        environment={'SG_EVENT_TEST_SOURCE_KEY': 'source-key'},
        directory=tmp_path,
        timeout_secs=0.2,
    )

    assert process.status(check='liveness')['liveness'] is True
    readiness = process.status(check='readiness')

    assert readiness['readiness'] is True
    assert readiness['detail'] == '<redacted>'
    assert process.last_status == readiness
    assert calls[0][-2:] == ('--check', 'liveness')
    assert calls[1][-2:] == ('--check', 'readiness')


def test_supervisor_stops_and_restarts_with_new_identity(tmp_path: Path) -> None:
    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml',
        environment={},
        directory=tmp_path,
        timeout_secs=STALL_DEADLINE_SECS,
        command=(sys.executable, '-c', 'import threading; threading.Event().wait()'),
    )
    assert process.stop() == ()

    try:
        process.start()
        first_pid = process.pid
        first_logs = process.log_paths
        assert first_pid is not None
        assert process.stop() == ()
        assert process.stop() == ()

        process.start()
        second_pid = process.pid
        second_logs = process.log_paths
        assert second_pid is not None
        assert second_pid != first_pid
        assert second_logs != first_logs
        assert process.stop() == ()
        assert all(path.is_file() for path in (*first_logs, *second_logs))
    finally:
        child = cast('subprocess.Popen[bytes] | None', vars(process)['process'])
        if child is not None and child.poll() is None:
            child.kill()
            child.wait()
        process.close_logs()


def test_unordered_probe_timeout_retains_last_inverted_callback_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = SupervisorProcess(
        config_path=tmp_path / 'config.toml',
        environment={},
        directory=tmp_path,
        timeout_secs=0.2,
    )
    live_case = _live_case(tmp_path, process)
    rows = [(102, 'two'), (101, 'one')]
    _write_probe_rows(live_case.output_paths['async'], 'async', rows)
    _write_probe_rows(live_case.output_paths['sync'], 'sync', rows)
    expected = (
        _live_event(101, 'one'),
        _live_event(102, 'two'),
        _live_event(103, 'three'),
    )
    monkeypatch.setattr(daemon_module, 'LIVE_TIMEOUT_SECS', 0.0)

    with pytest.raises(AssertionError, match='expected event set') as caught:
        wait_for_group_event_set(live_case, expected)

    diagnostic = json.loads(str(caught.value).partition('diagnostics=')[2])
    assert diagnostic['expected_event_ids'] == [101, 102, 103]
    assert [row['event_id'] for row in diagnostic['last_group_callback_rows']['async']] == [
        102,
        101,
    ]
