from __future__ import annotations

import errno
import importlib.metadata
import json
import logging
import os
import pwd
import signal
import socket
import ssl
import sys
import threading
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest

import sg_event_daemon._cli as cli_module
import sg_event_daemon._downstream as downstream_module
import sg_event_daemon._logging as logging_module
import sg_event_daemon._worker as worker_module
from sg_event_daemon._cli import _build_parser, main
from sg_event_daemon._config import load_acceptance_evidence, load_config
from sg_event_daemon._control import (
    ControlClient,
    ControlCommandError,
    ControlRequest,
    ControlServer,
    ControlUnavailableError,
)
from sg_event_daemon._events import EVENT_FIELDS
from sg_event_daemon._identity import stream_fingerprint
from sg_event_daemon._ipc import (
    ActiveSourceGroupAssignment,
    ControlHello,
    DeliveryGroupAssignment,
    EventPeerGrant,
    PendingDispositionSourceGroupAssignment,
    SourceEndpointReady,
    WorkerAssignment,
    WorkerControl,
)
from sg_event_daemon._platform import (
    PeerIdentity,
    ensure_secure_state_dir,
    reap_child,
    spawn_child,
    terminate_child,
)
from sg_event_daemon._probe import ProbeSnapshot
from sg_event_daemon._providers import ProviderCapabilities, ProviderError
from sg_event_daemon._source_state import SourceState
from sg_event_daemon._supervisor import Supervisor, _SupervisorState
from sg_event_daemon._worker import WorkerKind, WorkerSpec
from tests._ipc_support import partial_startup_child_code as _partial_startup_child_code
from tests._storage_support import open_group_on_source, write_context_for
from tests.cli_helpers import invoke_cli, process_transcript, run_cli, run_with_closed_output

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Mapping
    from typing import NoReturn, Self

    from sg_event_daemon._config import Config, CredentialConfig, GroupConfig, SiteConfig
    from sg_event_daemon._control import _ControlHandler
    from sg_event_daemon._ipc import SourceGroupAssignment
    from sg_event_daemon._probe import ProbeResult
    from sg_event_daemon._providers import Provider
    from sg_event_daemon._worker import WorkerRuntime

_FIXTURES = Path(__file__).parent / 'fixtures'

_RECOVERY_CASES = [
    (
        'retry',
        ['--site', 'test', '--group', 'editorial'],
        {'site': 'test', 'group': 'editorial'},
    ),
    (
        'skip-callback',
        [
            '--site',
            'test',
            '--group',
            'editorial',
            '--reason',
            'approved',
        ],
        {'site': 'test', 'group': 'editorial', 'reason': 'approved'},
    ),
    (
        'restart-event',
        [
            '--site',
            'test',
            '--group',
            'editorial',
            '--reason',
            'approved',
        ],
        {'site': 'test', 'group': 'editorial', 'reason': 'approved'},
    ),
    (
        'park',
        [
            '--site',
            'test',
            '--group',
            'editorial',
            '--reason',
            'maintenance',
        ],
        {'site': 'test', 'group': 'editorial', 'reason': 'maintenance'},
    ),
    (
        'unpark',
        [
            '--site',
            'test',
            '--group',
            'editorial',
            '--reason',
            'maintenance complete',
        ],
        {'site': 'test', 'group': 'editorial', 'reason': 'maintenance complete'},
    ),
    (
        'replay',
        [
            '--site',
            'test',
            '--group',
            'editorial',
            '--from-event-id',
            '123',
            '--acknowledge-truncation',
            '--reason',
            'approved',
        ],
        {
            'site': 'test',
            'group': 'editorial',
            'from_event_id': 123,
            'acknowledge_truncation': True,
            'reason': 'approved',
        },
    ),
    (
        'reset-group-to-latest',
        [
            '--site',
            'test',
            '--group',
            'editorial',
            '--reason',
            'discard',
        ],
        {'site': 'test', 'group': 'editorial', 'reason': 'discard'},
    ),
    (
        'adopt-source-stream',
        [
            '--site',
            'test',
            '--source',
            'admin',
            '--reason',
            'permissions changed',
        ],
        {'site': 'test', 'source': 'admin', 'reason': 'permissions changed'},
    ),
    (
        'acknowledge-ordering-anomaly',
        [
            '--site',
            'test',
            '--group',
            'editorial',
            '--reason',
            'approved',
        ],
        {'site': 'test', 'group': 'editorial', 'reason': 'approved'},
    ),
    (
        'reset-worker-breaker',
        [
            '--site',
            'test',
            '--worker',
            '["default","test"]',
            '--reason',
            'fixed',
        ],
        {'site': 'test', 'worker': '["default","test"]', 'reason': 'fixed'},
    ),
]
_VALIDATION_WARNING = (
    'warning: validation imports trusted plugin Python and may execute side effects\n'
)
_VALID_COMMAND_ARGUMENTS = ['status', '--config', 'daemon.toml']
_WORKER_ARGUMENTS = [
    'run-worker',
    '--config',
    'daemon.toml',
    '--site',
    'test',
    '--worker',
    '["site","test"]',
    '--control-fd',
    '7',
    '--spawn-instance',
    'spawn-1',
]


class _ClosedOutput:
    def flush(self) -> None:
        raise BrokenPipeError(errno.EPIPE, 'Broken pipe')

    def write(self, _text: str) -> int:
        raise BrokenPipeError(errno.EPIPE, 'Broken pipe')


class _CompletedWorkerRuntime:
    fatal_reason: str | None = None

    def request_stop(self, _reason: str) -> None:
        return None

    def start(self) -> None:
        return None

    def wait(self) -> tuple[()]:
        return ()


class _InterruptedWorkerRuntime:
    fatal_reason: str | None = None

    def __init__(self) -> None:
        self.stop_reasons: list[str] = []

    def request_stop(self, reason: str) -> None:
        self.stop_reasons.append(reason)

    def start(self) -> None:
        signal.raise_signal(signal.SIGINT)

    def wait(self) -> tuple[str, ...]:
        return ()


class _LiveHandle:
    def __init__(
        self,
        records: list[dict[str, object]] | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self._calls: list[tuple[str, object, list[str] | None, dict[str, object]]] = []
        self._closed = False
        self._error = error
        self._records = [_live_event(123)] if records is None else records

    def close(self) -> None:
        self._closed = True

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: object,
    ) -> list[dict[str, object]]:
        self._calls.append((entity_type, filters, fields, options))
        if self._error is not None:
            raise self._error
        return list(self._records)


class _LiveProvider:
    capabilities = ProviderCapabilities(sync=True, async_=True)

    def __init__(self, handle: _LiveHandle) -> None:
        self._handle = handle

    def create_sync(
        self,
        _site: str,
        _credential: CredentialConfig,
    ) -> _LiveHandle:
        return self._handle


class _OfflineControlServer:
    def __init__(self, _state_dir: Path, _handler: object) -> None:
        return None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_details: object) -> None:
        return None

    def serve_once(self, _timeout: float) -> bool:
        return False

    def set_foreground_progress(self, _callback: object) -> None:
        return None


class _OfflineProvider:
    capabilities = ProviderCapabilities(sync=True, async_=True)

    def create_sync(
        self,
        _site: str,
        _credential: CredentialConfig,
    ) -> NoReturn:
        raise AssertionError('offline validation made an SG request')


class _RecordingClient:
    def __init__(self, result: Mapping[str, object]) -> None:
        self.requests: list[tuple[str, Mapping[str, object] | None]] = []
        self._result = result

    def request(
        self,
        command: str,
        arguments: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        self.requests.append((command, arguments))
        return self._result


class _RefusingClient:
    def request(
        self,
        _command: str,
        _arguments: Mapping[str, object] | None = None,
    ) -> NoReturn:
        raise ControlCommandError('group is not parked')


class _StaticStatusClient:
    def __init__(self, result: Mapping[str, object]) -> None:
        self._result = result

    def request(
        self,
        _command: str,
        _arguments: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        return self._result


class _UnavailableStatusClient:
    def request(
        self,
        _command: str,
        _arguments: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        _raise_unavailable()


def _assigned_worker_config(tmp_path: Path, *, named: bool) -> Path:
    group_names = ('d1', 'd2') if named else ('g1', 'g2')
    path = tmp_path / 'assigned-worker.toml'
    path.write_text(
        '\n'.join(
            (
                '[daemon]',
                f'state_dir = "{tmp_path / "state"}"',
                'sg_api_provider = "shotgun-api3"',
                '',
                '[sites.test]',
                'server = "https://example.invalid"',
                'default_source = "s"',
                '',
                '[sites.test.credentials.s]',
                'script_name = "test"',
                'api_key = { env = "SG_EVENT_ADMIN_KEY" }',
                '',
                '[sites.test.sources.s]',
                'credentials = "s"',
                *(('', '[sites.test.workers.pool]') if named else ()),
                *(
                    line
                    for group in group_names
                    for line in (
                        '',
                        f'[sites.test.groups.{group}]',
                        *(('worker = "pool"',) if named else ()),
                        '',
                        f'[[sites.test.groups.{group}.plugins]]',
                        'id = "noop"',
                        f'path = "{_FIXTURES / "noop_plugin.py"}"',
                    )
                ),
            )
        )
        + '\n',
        encoding='utf-8',
    )
    return path


def _control_config(tmp_path: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    state_dir = tmp_path / 'state'
    path = tmp_path / 'control.toml'
    path.write_text(
        f'[daemon]\nstate_dir = "{state_dir}"\n',
        encoding='utf-8',
    )
    return path


def _fail_runtime(
    _request: ControlRequest,
    _identity: PeerIdentity,
) -> NoReturn:
    raise RuntimeError('revision conflicts')


def _fixed_version(_name: str) -> str:
    return '9.8.7'


def _interrupt(*_args: object) -> NoReturn:
    raise KeyboardInterrupt


@pytest.fixture(autouse=True)
def _isolated_redaction(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(logging_module, '_ordered_redaction_values', ())
    monkeypatch.setattr(logging_module, '_REDACTION_VALUES', set[str]())


def _keep_descriptor(_stream: object) -> bool:
    return True


def _live_event(event_id: int) -> dict[str, object]:
    return {
        'id': event_id,
        'event_type': 'Shotgun_Task_Change',
        'attribute_name': 'sg_status_list',
        'meta': {},
        'entity': None,
        'user': None,
        'project': None,
        'session_uuid': None,
        'created_at': datetime(2026, 7, 29, 12, 0, tzinfo=UTC),
        'type': 'EventLogEntry',
    }


def _load_no_plugins(
    _site: SiteConfig,
    _group: GroupConfig,
    _provider: Provider,
) -> tuple[()]:
    return ()


def _load_offline_provider(
    _name: str,
    _required: Collection[str],
) -> _OfflineProvider:
    return _OfflineProvider()


def _named_error(name: str, base: type[Exception] = Exception) -> type[Exception]:
    return type(name, (base,), {})


def _offline_worker(monkeypatch: pytest.MonkeyPatch, runtime: object) -> None:
    config = SimpleNamespace(
        daemon=SimpleNamespace(
            health_interval_secs=0.25,
            max_ipc_frame_bytes=131_072,
            worker_startup_deadline_secs=0.75,
        )
    )
    assignment = WorkerAssignment(
        worker_id='["site","test"]',
        spawn_instance='spawn-1',
        source_generations=(),
        group_assignments=(),
        source_group_assignments=(),
        source_endpoints=(),
        event_grants=(),
    )

    class _Control:
        def close(self) -> None:
            return None

        def receive(self, *, timeout: float) -> WorkerAssignment:
            assert timeout == 0.75
            return assignment

        def send(self, _message: object) -> None:
            return None

    def load_offline_config(_path: Path) -> object:
        return config

    def worker_spec(_config: object, _site: str, worker: str) -> object:
        return WorkerSpec(
            worker_id=worker,
            site='test',
            kind=WorkerKind._SITE,
            name='test',
            sources=(),
            groups=(),
        )

    def control_from_fd(_descriptor: int, **_options: object) -> _Control:
        return _Control()

    def build_runtime(*_args: object) -> object:
        return runtime

    def ignore_logging(**_options: object) -> None:
        return None

    monkeypatch.setattr(cli_module, 'load_config', load_offline_config)
    monkeypatch.setattr(cli_module, '_worker_spec', worker_spec)
    monkeypatch.setattr(cli_module.WorkerControl, 'from_fd', control_from_fd)
    monkeypatch.setattr(cli_module, 'build_worker_runtime', build_runtime)
    monkeypatch.setattr(cli_module, 'configure_logging', ignore_logging)


def _raise_unavailable() -> NoReturn:
    raise ControlUnavailableError('supervisor is not running')


def _run_assigned_worker(
    monkeypatch: pytest.MonkeyPatch,
    config_path: Path,
    assignment: WorkerAssignment,
) -> tuple[tuple[int, str, str], list[WorkerSpec]]:
    parent_socket, child_socket = socket.socketpair()
    parent = WorkerControl(parent_socket, max_frame_bytes=8_388_608, send_deadline_secs=1.0)
    child = WorkerControl(child_socket, max_frame_bytes=8_388_608, send_deadline_secs=1.0)
    recorded: list[WorkerSpec] = []

    def control_from_fd(_descriptor: int, **_options: object) -> WorkerControl:
        return child

    def build_runtime(
        config: Config,
        spec: WorkerSpec,
        received: WorkerAssignment,
        control: WorkerControl,
    ) -> WorkerRuntime:
        worker_module.build_worker_runtime(config, spec, received, control)
        recorded.append(spec)
        return cast('WorkerRuntime', _CompletedWorkerRuntime())

    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    monkeypatch.setattr(cli_module.WorkerControl, 'from_fd', control_from_fd)
    monkeypatch.setattr(cli_module, 'build_worker_runtime', build_runtime)
    parent.send(assignment)
    try:
        result = invoke_cli(
            [
                'run-worker',
                '--config',
                os.fspath(config_path),
                '--site',
                'test',
                '--worker',
                assignment.worker_id,
                '--control-fd',
                str(child_socket.fileno()),
                '--spawn-instance',
                assignment.spawn_instance,
            ]
        )
    finally:
        parent.close()
        child.close()
    return result, recorded


def _run_server(server: ControlServer, stop: threading.Event) -> None:
    while not stop.is_set():
        server.serve_once(0.1)


def _run_with_control_server(
    tmp_path: Path,
    argv: list[str],
    handler: _ControlHandler,
) -> int:
    config_path = _control_config(tmp_path)
    stop = threading.Event()
    with ControlServer(config_path.parent / 'state', handler) as server:
        thread = threading.Thread(target=_run_server, args=(server, stop))
        try:
            thread.start()
            return main(
                [
                    *argv,
                    '--config',
                    os.fspath(config_path),
                ]
            )
        finally:
            stop.set()
            if thread.ident is not None:
                thread.join(2)
                assert not thread.is_alive()


def _set_live_provider(
    monkeypatch: pytest.MonkeyPatch,
    handle: _LiveHandle,
) -> None:
    def load_live_provider(
        _name: str,
        _required: Collection[str],
    ) -> _LiveProvider:
        return _LiveProvider(handle)

    monkeypatch.setattr(cli_module, 'load_provider', load_live_provider)


def _set_status_client(
    monkeypatch: pytest.MonkeyPatch,
    client: object,
) -> None:
    def for_state_dir(
        _cls: type[ControlClient],
        _state_dir: Path,
        *,
        timeout: float = 5.0,
    ) -> object:
        del timeout
        return client

    monkeypatch.setattr(
        cli_module.ControlClient,
        'for_state_dir',
        classmethod(for_state_dir),
    )


def _supervise_config(tmp_path: Path, *, probe_port: int | None = None) -> Path:
    plugin = _FIXTURES / 'noop_plugin.py'
    path = tmp_path / 'supervise.toml'
    path.write_text(
        '\n'.join(
            (
                '[daemon]',
                f'state_dir = "{tmp_path / "state"}"',
                'sg_api_provider = "shotgun-api3"',
                *(
                    ('probe_bind = "127.0.0.1"', f'probe_port = {probe_port}')
                    if probe_port
                    else ()
                ),
                '',
                '[sites.test]',
                'server = "https://example.invalid"',
                'default_source = "admin"',
                '',
                '[sites.test.credentials.admin]',
                'script_name = "test"',
                'api_key = { env = "SG_EVENT_ADMIN_KEY" }',
                '',
                '[sites.test.sources.admin]',
                'credentials = "admin"',
                '',
                '[sites.test.workers.pool]',
                '',
                '[sites.test.groups.remote]',
                'worker = "pool"',
                '',
                '[[sites.test.groups.remote.plugins]]',
                'id = "noop"',
                f'path = "{plugin}"',
            )
        )
        + '\n',
        encoding='utf-8',
    )
    return path


def _offline_supervise_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    run: Callable[..., None],
) -> Path:
    def skip_provider_validation(_config: Config) -> None:
        return None

    def start_supervisor(_supervisor: Supervisor) -> None:
        return None

    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    monkeypatch.setattr(cli_module, 'ControlServer', _OfflineControlServer)
    monkeypatch.setattr(cli_module, '_validated_provider', skip_provider_validation)
    monkeypatch.setattr(Supervisor, 'start', start_supervisor)
    monkeypatch.setattr(Supervisor, 'run', run)
    return _supervise_config(tmp_path)


def test_closed_stdout_before_help_is_quiet() -> None:
    result = run_with_closed_output(['--help'])
    assert (result.returncode, result.stderr) == (0, b''), process_transcript(result)


def test_closed_stdout_drops_supervisor_log_lines(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def log_then_stop(_supervisor: Supervisor, **_options: object) -> None:
        logging.getLogger('sg_event_daemon.test').warning('callback finished')

    config_path = _offline_supervise_config(monkeypatch, tmp_path, log_then_stop)
    monkeypatch.setattr(downstream_module, '_silence_descriptor', _keep_descriptor)
    monkeypatch.setattr(sys, 'stdout', _ClosedOutput())

    result = main(['supervise', '--config', os.fspath(config_path)])

    assert (result, capsys.readouterr().err) == (0, '')


def test_closed_stdout_during_validation_keeps_its_status() -> None:
    result = run_with_closed_output(
        ['validate-config', '--config', os.fspath(_FIXTURES / 'valid.toml')],
        env={**os.environ, 'SG_EVENT_ADMIN_KEY': 'test-key'},
    )
    assert (result.returncode, result.stderr) == (
        0,
        _VALIDATION_WARNING.encode(),
    ), process_transcript(result)


def test_command_help_names_option_values() -> None:
    status, stdout, stderr = invoke_cli(['park', '--help'])
    assert (status, stderr) == (0, '')
    assert ' '.join(stdout.split('\n\n', 1)[0].split()) == (
        'usage: sg-event-daemon park [-h] --config FILE --site SITE --group GROUP --reason REASON'
    )


def test_configuration_that_is_not_utf8_is_invalid_toml(tmp_path: Path) -> None:
    path = tmp_path / 'daemon.toml'
    path.write_bytes(b'\xff\xfe')
    assert invoke_cli(['status', '--config', os.fspath(path)]) == (
        2,
        '',
        f'error: invalid TOML in {path}: the file is not UTF-8 text\n',
    )


def test_conflicting_worker_assignment_exits_1(monkeypatch: pytest.MonkeyPatch) -> None:
    _offline_worker(monkeypatch, _InterruptedWorkerRuntime())

    status = invoke_cli(
        [
            'run-worker',
            '--config',
            'daemon.toml',
            '--site',
            'test',
            '--worker',
            '["site","other"]',
            '--control-fd',
            '7',
            '--spawn-instance',
            'spawn-1',
        ]
    )

    assert status == (1, '', 'error: worker assignment identity conflicts with argv\n')


def test_control_commands_never_load_or_resolve_full_configuration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fail_load(_path: Path) -> None:
        raise AssertionError('control command loaded secrets')

    monkeypatch.setattr(cli_module, 'load_config', fail_load)

    result = _run_with_control_server(
        tmp_path,
        ['status'],
        lambda _request, _identity: {'liveness': True, 'readiness': True},
    )

    assert result == 0


@pytest.mark.parametrize(
    ('command', 'arguments', 'expected_arguments'),
    [
        *_RECOVERY_CASES,
        (
            'status',
            ['--site', 'test', '--group', 'editorial'],
            {'site': 'test', 'group': 'editorial'},
        ),
    ],
)
def test_control_commands_send_only_their_own_arguments(
    arguments: list[str],
    command: str,
    expected_arguments: Mapping[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _RecordingClient({'accepted': True})
    _set_status_client(monkeypatch, client)

    status, stdout, stderr = invoke_cli(
        [command, *arguments, '--config', os.fspath(_control_config(tmp_path))]
    )

    assert (status, stdout, stderr) == (0, '{"accepted":true}\n', '')
    assert client.requests == [(command, expected_arguments)]


def test_error_lines_escape_control_characters(tmp_path: Path) -> None:
    path = tmp_path / 'daemon\x1b[31m.toml'
    status, stdout, stderr = invoke_cli(['status', '--config', os.fspath(path)])
    assert (status, stdout) == (2, '')
    assert stderr == (
        f'error: cannot read configuration file {tmp_path}/daemon\\x1b[31m.toml: '
        'No such file or directory\n'
    )


@pytest.mark.parametrize(
    ('result', 'expected'),
    [
        (
            {'liveness': False, 'readiness': False},
            (1, '{"liveness":false,"readiness":false}\n', ''),
        ),
        (
            {'readiness': True},
            (1, '{"readiness":true}\n', "error: status result has no boolean 'liveness' field\n"),
        ),
    ],
)
def test_failed_status_check_prints_its_result_and_exits_1(
    expected: tuple[int, str, str],
    monkeypatch: pytest.MonkeyPatch,
    result: Mapping[str, object],
    tmp_path: Path,
) -> None:
    _set_status_client(monkeypatch, _StaticStatusClient(result))
    assert (
        invoke_cli(
            ['status', '--config', os.fspath(_control_config(tmp_path)), '--check', 'liveness']
        )
        == expected
    )


def test_help_goes_to_stdout() -> None:
    status, stdout, stderr = invoke_cli(['--help'])
    assert (status, stderr) == (0, '')
    assert stdout.startswith('usage: sg-event-daemon [-h] [--version] COMMAND ...\n')


def test_help_lists_commands_in_titled_groups(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('COLUMNS', '100')
    monkeypatch.setenv('PYTHON_COLORS', '0')
    help_text = _build_parser().format_help()
    lines = help_text.splitlines()
    headings = [line for line in lines if line.endswith(':') and not line.startswith(' ')]
    assert headings == ['options:', 'run:', 'recover a group:', 'recover a source or worker:']
    expected = {
        'run:': {'supervise', 'validate-config', 'status'},
        'recover a group:': {
            'retry',
            'skip-callback',
            'restart-event',
            'park',
            'unpark',
            'replay',
            'reset-group-to-latest',
            'acknowledge-ordering-anomaly',
        },
        'recover a source or worker:': {'adopt-source-stream', 'reset-worker-breaker'},
    }
    for index, (heading, commands) in enumerate(expected.items(), start=1):
        start = lines.index(heading) + 1
        end = lines.index(headings[index + 1]) if index + 1 < len(headings) else len(lines)
        first_words = {line.split()[0] for line in lines[start:end] if line.split()}
        assert commands <= first_words


@pytest.mark.parametrize(
    ('argv', 'loader'),
    [
        (_VALID_COMMAND_ARGUMENTS, 'load_control_config'),
        (['validate-config', '--config', 'daemon.toml'], 'load_config'),
        (['supervise', '--config', 'daemon.toml'], 'load_config'),
        (_WORKER_ARGUMENTS, 'load_config'),
    ],
)
def test_interrupt_outside_a_running_service_exits_130_quietly(
    argv: list[str],
    loader: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli_module, loader, _interrupt)
    assert invoke_cli(argv) == (130, '', '')


def test_interrupt_while_running_a_worker_stops_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _InterruptedWorkerRuntime()
    _offline_worker(monkeypatch, runtime)

    assert invoke_cli(_WORKER_ARGUMENTS) == (0, '', '')
    assert runtime.stop_reasons == ['foreground-sigint']


def test_interrupt_while_supervising_stops_cleanly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    observed: list[bool] = []

    def interrupt_then_stop(
        _supervisor: Supervisor,
        *,
        should_stop: Callable[[], bool],
        **_options: object,
    ) -> None:
        signal.raise_signal(signal.SIGINT)
        observed.append(should_stop())

    config_path = _offline_supervise_config(monkeypatch, tmp_path, interrupt_then_stop)

    status, _stdout, stderr = invoke_cli(['supervise', '--config', os.fspath(config_path)])

    assert (status, stderr, observed) == (0, '', [True])


def test_invalid_configuration_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv('SG_EVENT_ADMIN_KEY', raising=False)
    assert invoke_cli(['validate-config', '--config', os.fspath(_FIXTURES / 'valid.toml')]) == (
        2,
        '',
        (
            'error: sites.test.credentials.admin.api_key environment variable '
            'SG_EVENT_ADMIN_KEY is not set\n'
        ),
    )


@pytest.mark.parametrize(
    ('error', 'detail'),
    [
        pytest.param(
            _named_error(
                'AuthenticationFault', _named_error('Fault', _named_error('ShotgunError'))
            )("Can't authenticate script 'test'"),
            "Can't authenticate script 'test'",
            id='authentication',
        ),
        pytest.param(
            _named_error('ProtocolError')(
                '<ProtocolError for example.invalid/api3/json: 503 Service Unavailable>'
            ),
            '<ProtocolError for example.invalid/api3/json: 503 Service Unavailable>',
            id='http-status',
        ),
        pytest.param(
            _named_error('ServerNotFoundError', _named_error('HttpLib2Error'))(
                'Unable to find the server at example.invalid'
            ),
            'Unable to find the server at example.invalid',
            id='unknown-host',
        ),
        pytest.param(
            ConnectionRefusedError(61, 'Connection refused'),
            '[Errno 61] Connection refused',
            id='connection-refused',
        ),
        pytest.param(
            ssl.SSLCertVerificationError(1, 'certificate verify failed'),
            'certificate verify failed',
            id='tls',
        ),
        pytest.param(TimeoutError(), 'TimeoutError', id='timeout-without-message'),
        pytest.param(
            _named_error('TransportError', _named_error('ShotgunError'))('first\nsecond'),
            'first\\x0asecond',
            id='multi-line-message',
        ),
    ],
)
def test_live_provider_failure_is_a_runtime_error(
    detail: str,
    error: Exception,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    handle = _LiveHandle(error=error)
    _set_live_provider(monkeypatch, handle)
    monkeypatch.setattr(cli_module, 'load_plugins', _load_no_plugins)
    _set_status_client(monkeypatch, _UnavailableStatusClient())

    status, stdout, stderr = invoke_cli(
        ['validate-config', '--config', os.fspath(_FIXTURES / 'valid.toml'), '--live']
    )

    assert (status, stdout) == (1, '')
    assert stderr.endswith(f'error: live source test/<redacted> request failed: {detail}\n')
    assert handle._closed


def test_livez_probe_listener_exit_is_fatal_to_supervise(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    config_path = _supervise_config(tmp_path, probe_port=8_080)

    class FailedProbeServer:
        def __init__(self, *_args: object, **_options: object) -> None:
            return None

        @property
        def failure_reason(self) -> str:
            return 'probe listener exited unexpectedly'

        def close(self) -> None:
            return None

        def join(self, *, timeout: float) -> bool:
            return timeout >= 0

        def start(self) -> None:
            return None

    def observe_stop(
        _supervisor: Supervisor,
        *,
        should_stop: Callable[[], bool],
        wait: Callable[[float], object],
        set_foreground_progress: Callable[[Callable[[], None] | None], None],
    ) -> None:
        control = getattr(wait, '__self__', None)
        assert isinstance(control, ControlServer)
        assert set_foreground_progress == control.set_foreground_progress
        should_stop()

    def skip_provider_validation(_config: Config) -> None:
        return None

    def start_supervisor(_supervisor: Supervisor) -> None:
        return None

    monkeypatch.setattr(cli_module, 'ProbeServer', FailedProbeServer, raising=False)
    monkeypatch.setattr(cli_module, '_validated_provider', skip_provider_validation)
    monkeypatch.setattr(Supervisor, 'start', start_supervisor)
    monkeypatch.setattr(Supervisor, 'run', observe_stop)

    result = main(['supervise', '--config', os.fspath(config_path)])

    assert result == 1
    assert 'probe listener exited unexpectedly' in capsys.readouterr().err


def test_main_rejects_unknown_worker_without_starting_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    parent_socket, child_socket = socket.socketpair()
    try:
        result = invoke_cli(
            [
                'run-worker',
                '--config',
                os.fspath(_FIXTURES / 'valid.toml'),
                '--site',
                'test',
                '--worker',
                '["named","test","missing"]',
                '--control-fd',
                str(child_socket.fileno()),
                '--spawn-instance',
                'spawn-1',
            ]
        )
    finally:
        parent_socket.close()
        child_socket.close()

    assert result == (2, '', 'error: unknown worker for site test: ["named","test","missing"]\n')


def test_malformed_configuration_is_invalid_toml(tmp_path: Path) -> None:
    path = tmp_path / 'daemon.toml'
    path.write_text('[daemon\n', encoding='utf-8')
    status, stdout, stderr = invoke_cli(['status', '--config', os.fspath(path)])
    assert (status, stdout) == (2, '')
    assert stderr.startswith(f'error: invalid TOML in {path}: ')
    assert stderr.count('\n') == 1


def test_missing_command_is_a_usage_error() -> None:
    status, stdout, stderr = invoke_cli([])
    assert (status, stdout) == (2, '')
    assert stderr.endswith(
        'sg-event-daemon: error: the following arguments are required: COMMAND\n'
    )


def test_missing_metadata_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing(name: str) -> str:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, 'version', missing)
    status, stdout, stderr = invoke_cli(['--version'])
    assert (status, stdout) == (2, '')
    assert stderr == 'error: installed distribution metadata for sg-event-daemon is unavailable\n'


def test_missing_option_is_a_usage_error_for_its_command() -> None:
    status, stdout, stderr = invoke_cli(['status'])
    assert (status, stdout) == (2, '')
    assert stderr.startswith('usage: sg-event-daemon status [-h] --config FILE')
    assert stderr.endswith(
        'sg-event-daemon status: error: the following arguments are required: --config\n'
    )


def test_module_entry_point_runs_main() -> None:
    result = run_cli(['--help'], expect=0)
    assert result.stdout.startswith('usage: sg-event-daemon '), process_transcript(result)


def test_negative_control_descriptor_is_an_argument_error() -> None:
    assert invoke_cli(
        [
            'run-worker',
            '--config',
            'daemon.toml',
            '--site',
            'test',
            '--worker',
            '["site","test"]',
            '--control-fd',
            '-1',
            '--spawn-instance',
            'spawn-1',
        ]
    ) == (2, '', 'error: control descriptor must be nonnegative\n')


def test_normal_runs_do_not_read_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(name: str) -> str:
        raise AssertionError(name)

    monkeypatch.setattr(importlib.metadata, 'version', forbidden)
    assert invoke_cli(['--help'])[0] == 0


@pytest.mark.parametrize(
    ('argv', 'command'),
    [
        (['supervise', '--config', 'config.toml'], 'supervise'),
        (
            [
                'run-worker',
                '--config',
                'config.toml',
                '--site',
                'test',
                '--worker',
                '["site","test"]',
                '--control-fd',
                '7',
                '--spawn-instance',
                'spawn-1',
            ],
            'run-worker',
        ),
        (['status', '--config', 'config.toml'], 'status'),
        (
            [
                'status',
                '--config',
                'config.toml',
                '--site',
                'test',
                '--group',
                'editorial',
                '--check',
                'readiness',
            ],
            'status',
        ),
        (['validate-config', '--config', 'config.toml'], 'validate-config'),
        (
            [
                'validate-config',
                '--config',
                'config.toml',
                '--live',
                '--evidence',
                'evidence.toml',
            ],
            'validate-config',
        ),
    ],
)
def test_parser_accepts_documented_command_forms(
    argv: list[str],
    command: str,
) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    assert args.command == command


def test_plugin_import_failure_is_a_configuration_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    monkeypatch.setattr(cli_module, 'load_provider', _load_offline_provider)
    config_path = tmp_path / 'daemon.toml'
    config_path.write_text(
        (_FIXTURES / 'valid.toml').read_text(encoding='utf-8'), encoding='utf-8'
    )
    (tmp_path / 'noop_plugin.py').write_text("raise RuntimeError('boom')\n", encoding='utf-8')

    assert invoke_cli(['validate-config', '--config', os.fspath(config_path)]) == (
        2,
        '',
        (
            f"{_VALIDATION_WARNING}error: import-failed for site 'test', group 'noop', "
            "plugin 'noop': plugin import failed: boom; importing trusted plugin code "
            'executes top-level Python; it is not sandboxed or guaranteed side-effect-free\n'
        ),
    )


def test_probe_bind_failure_prevents_supervisor_start(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    config_path = _supervise_config(tmp_path, probe_port=8_080)
    starts: list[str] = []

    def fail_probe_bind(*_args: object, **_options: object) -> NoReturn:
        raise OSError('probe bind failed')

    def record_start(_supervisor: Supervisor) -> None:
        starts.append('started')

    def skip_provider_validation(_config: Config) -> None:
        return None

    monkeypatch.setattr(cli_module, 'ProbeServer', fail_probe_bind, raising=False)
    monkeypatch.setattr(cli_module, '_validated_provider', skip_provider_validation)
    monkeypatch.setattr(Supervisor, 'start', record_start)

    result = main(['supervise', '--config', os.fspath(config_path)])

    assert result == 1
    assert starts == []
    assert 'probe bind failed' in capsys.readouterr().err


def test_probe_handler_skips_audit_and_full_status_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_boundary(*_args: object, **_kwargs: object) -> NoReturn:
        raise AssertionError('probe entered an audited or full-status boundary')

    monkeypatch.setattr(cli_module, '_audit_identity', fail_boundary)
    for name in ('OperatorCommand', 'Path', 'WorkerControl', 'load_plugins', 'load_provider'):
        monkeypatch.setattr(cli_module, name, fail_boundary)
    for name in (
        'adopt_source_stream_command',
        'dispatch_group_disposition_command',
        'dispatch_operator_command',
        'reset_worker_breaker_command',
        'status_mapping',
    ):
        monkeypatch.setattr(Supervisor, name, fail_boundary)
    supervisor = object.__new__(Supervisor)
    observations = 0

    def probe_mapping() -> Mapping[str, object]:
        nonlocal observations
        observations += 1
        return {
            'liveness': True,
            'readiness': False,
            'complete': True,
            'conditions': ('source-degraded',),
            'generated_at': 10.0,
            'snapshot_age_seconds': 0.25,
        }

    vars(supervisor)['probe_mapping'] = probe_mapping
    handle_request = vars(cli_module)['_handle_supervisor_request']
    result = handle_request(
        supervisor,
        ControlRequest(
            _version=1,
            request_id='probe-1',
            command='probe',
            arguments={},
        ),
        cast('PeerIdentity', object()),
    )

    assert result['liveness'] is True
    assert observations == 1


def test_probe_join_failure_does_not_replace_supervisor_close_failure(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    config_path = _supervise_config(tmp_path, probe_port=8_080)
    original_close = Supervisor._close
    cleanup: list[str] = []

    class FailedCleanupProbeServer:
        def __init__(self, *_args: object, **_options: object) -> None:
            return None

        @property
        def failure_reason(self) -> None:
            return None

        def close(self) -> None:
            cleanup.append('close')

        def join(self, *, timeout: float) -> bool:
            cleanup.append(f'join:{timeout >= 0}')
            return False

        def start(self) -> None:
            return None

    def close_then_fail(supervisor: Supervisor) -> NoReturn:
        original_close(supervisor)
        raise RuntimeError('supervisor close failed')

    def skip_provider_validation(_config: Config) -> None:
        return None

    def start_supervisor(_supervisor: Supervisor) -> None:
        return None

    def stop_without_run(_supervisor: Supervisor, **_options: object) -> None:
        return None

    monkeypatch.setattr(cli_module, 'ProbeServer', FailedCleanupProbeServer)
    monkeypatch.setattr(cli_module, '_validated_provider', skip_provider_validation)
    monkeypatch.setattr(Supervisor, '_close', close_then_fail)
    monkeypatch.setattr(Supervisor, 'start', start_supervisor)
    monkeypatch.setattr(Supervisor, 'run', stop_without_run)

    result = main(['supervise', '--config', os.fspath(config_path)])

    assert result == 1
    assert cleanup == ['close', 'join:True']
    captured = capsys.readouterr()
    assert 'supervisor close failed' in captured.err
    assert 'probe listener shutdown deadline exceeded' not in captured.err


def test_probe_join_failure_does_not_replace_supervisor_run_failure(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    config_path = _supervise_config(tmp_path, probe_port=8_080)
    cleanup: list[str] = []

    class FailedCleanupProbeServer:
        def __init__(self, *_args: object, **_options: object) -> None:
            return None

        @property
        def failure_reason(self) -> None:
            return None

        def close(self) -> None:
            cleanup.append('close')

        def join(self, *, timeout: float) -> bool:
            cleanup.append(f'join:{timeout >= 0}')
            raise RuntimeError('probe join failed')

        def start(self) -> None:
            return None

    def fail_run(_supervisor: Supervisor, **_options: object) -> NoReturn:
        raise RuntimeError('supervisor run failed')

    def skip_provider_validation(_config: Config) -> None:
        return None

    def start_supervisor(_supervisor: Supervisor) -> None:
        return None

    monkeypatch.setattr(cli_module, 'ProbeServer', FailedCleanupProbeServer)
    monkeypatch.setattr(cli_module, '_validated_provider', skip_provider_validation)
    monkeypatch.setattr(Supervisor, 'start', start_supervisor)
    monkeypatch.setattr(Supervisor, 'run', fail_run)

    result = main(['supervise', '--config', os.fspath(config_path)])

    assert result == 1
    assert cleanup == ['close', 'join:True']
    captured = capsys.readouterr()
    assert 'supervisor run failed' in captured.err
    assert 'probe join failed' not in captured.err


def test_probe_port_omission_constructs_no_listener(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    config_path = _supervise_config(tmp_path)

    def fail_probe_construction(*_args: object, **_options: object) -> NoReturn:
        raise AssertionError('disabled HTTP probe constructed a listener')

    def skip_provider_validation(_config: Config) -> None:
        return None

    def start_supervisor(_supervisor: Supervisor) -> None:
        return None

    def stop_without_run(_supervisor: Supervisor, **_options: object) -> None:
        return None

    monkeypatch.setattr(cli_module, 'ProbeServer', fail_probe_construction, raising=False)
    monkeypatch.setattr(cli_module, '_validated_provider', skip_provider_validation)
    monkeypatch.setattr(Supervisor, 'start', start_supervisor)
    monkeypatch.setattr(Supervisor, 'run', stop_without_run)

    result = main(['supervise', '--config', os.fspath(config_path)])

    assert result == 0


def test_readyz_becomes_unavailable_before_probe_listener_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    config_path = _supervise_config(tmp_path, probe_port=8_080)
    readiness_at_close: list[bool] = []
    join_timeouts: list[float] = []

    class RecordingProbeServer:
        def __init__(
            self,
            _bind: str,
            _port: int,
            result: Callable[[], ProbeResult],
            **_options: object,
        ) -> None:
            self._result = result

        @property
        def failure_reason(self) -> None:
            return None

        def close(self) -> None:
            probe = self._result()
            readiness_at_close.append(probe.readiness)

        def join(self, *, timeout: float) -> bool:
            join_timeouts.append(timeout)
            return True

        def start(self) -> None:
            return None

    def skip_provider_validation(_config: Config) -> None:
        return None

    def start_supervisor(supervisor: Supervisor) -> None:
        progress = supervisor._progress
        now = supervisor._clock()
        progress.advance(now=now)
        progress.publish(
            ProbeSnapshot(
                generated_at=now,
                ready=True,
                complete=True,
                conditions=('healthy',),
                shutting_down=False,
                halted=False,
            )
        )
        ready_probe = supervisor.probe_result()
        assert ready_probe.complete
        assert ready_probe.readiness

    def stop_without_run(_supervisor: Supervisor, **_options: object) -> None:
        return None

    monkeypatch.setattr(cli_module, 'ProbeServer', RecordingProbeServer, raising=False)
    monkeypatch.setattr(cli_module, '_validated_provider', skip_provider_validation)
    monkeypatch.setattr(Supervisor, 'start', start_supervisor)
    monkeypatch.setattr(Supervisor, 'run', stop_without_run)

    result = main(['supervise', '--config', os.fspath(config_path)])

    assert result == 0
    assert readiness_at_close == [False]
    assert len(join_timeouts) == 1
    assert 0.0 <= join_timeouts[0] <= 10.0


@pytest.mark.parametrize(('command', 'arguments', 'expected_arguments'), _RECOVERY_CASES)
def test_recovery_commands_route_body_free_payloads_through_control_supervisor(
    arguments: list[str],
    command: str,
    expected_arguments: Mapping[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv('SG_EVENT_ADMIN_KEY', raising=False)
    observed: list[tuple[ControlRequest, PeerIdentity]] = []

    def handler(
        request: ControlRequest,
        identity: PeerIdentity,
    ) -> Mapping[str, object]:
        observed.append((request, identity))
        return {'accepted': True, 'command': request.command}

    result = _run_with_control_server(
        tmp_path,
        [command, *arguments],
        handler,
    )

    assert result == 0
    request, identity = observed[0]
    assert request.command == command
    assert request.arguments == expected_arguments
    assert 'event' not in request.arguments
    assert 'local_uid' not in request.arguments
    assert identity.uid == os.geteuid()


def test_refused_control_command_exits_1(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _set_status_client(monkeypatch, _RefusingClient())
    assert invoke_cli(
        [
            'unpark',
            '--config',
            os.fspath(_control_config(tmp_path)),
            '--site',
            'test',
            '--group',
            'editorial',
            '--reason',
            'maintenance complete',
        ]
    ) == (1, '', 'error: group is not parked\n')


def test_refused_recovery_command_exits_1_through_the_control_server(tmp_path: Path) -> None:
    result = _run_with_control_server(
        tmp_path,
        ['park', '--site', 'test', '--group', 'editorial', '--reason', 'race'],
        _fail_runtime,
    )

    assert result == 1


def test_run_worker_rejects_group_outside_configured_worker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = _assigned_worker_config(tmp_path, named=True)
    assignment = WorkerAssignment(
        worker_id='["named","test","pool"]',
        spawn_instance='spawn-1',
        source_generations=(),
        group_assignments=(DeliveryGroupAssignment('s', 'other', 1, 'resume'),),
        source_group_assignments=(),
        source_endpoints=(
            SourceEndpointReady('s', 'source-1', os.fspath(tmp_path / 'endpoint.sock')),
        ),
        event_grants=(EventPeerGrant('s', 'other', 'spawn-1', 'capability'),),
    )

    result, recorded = _run_assigned_worker(monkeypatch, config_path, assignment)

    assert result[0] != 0
    assert 'worker assignment groups conflict with configured worker' in result[2]
    assert recorded == []


def test_run_worker_starts_dependent_with_pending_group(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = _assigned_worker_config(tmp_path, named=True)
    assignment = WorkerAssignment(
        worker_id='["named","test","pool"]',
        spawn_instance='spawn-1',
        source_generations=(),
        group_assignments=(DeliveryGroupAssignment('s', 'd1', 1, 'resume'),),
        source_group_assignments=(),
        source_endpoints=(
            SourceEndpointReady('s', 'source-1', os.fspath(tmp_path / 'endpoint.sock')),
        ),
        event_grants=(EventPeerGrant('s', 'd1', 'spawn-1', 'capability'),),
    )

    result, recorded = _run_assigned_worker(monkeypatch, config_path, assignment)

    assert result == (0, '', '')
    assert len(recorded) == 1
    assert recorded[0].groups == ('d1',)


def test_run_worker_starts_source_owner_with_pending_group(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = _assigned_worker_config(tmp_path, named=False)
    assignment = WorkerAssignment(
        worker_id='["site","test"]',
        spawn_instance='spawn-1',
        source_generations=(('s', 'source-1'),),
        group_assignments=(DeliveryGroupAssignment('s', 'g1', 1, 'resume'),),
        source_group_assignments=(
            ActiveSourceGroupAssignment('s', 'g1', 1, 'resume'),
            PendingDispositionSourceGroupAssignment('s', 'g2', 1, 2),
        ),
        source_endpoints=(),
        event_grants=(),
    )

    result, recorded = _run_assigned_worker(monkeypatch, config_path, assignment)

    assert result == (0, '', '')
    assert len(recorded) == 1
    assert recorded[0].groups == ('g1',)
    assert recorded[0].sources == ('s',)


def test_runtime_value_error_exits_1(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fail_run(_supervisor: Supervisor, **_options: object) -> NoReturn:
        raise ValueError('worker source inventory conflicts with current assignment')

    config_path = _offline_supervise_config(monkeypatch, tmp_path, fail_run)

    status, _stdout, stderr = invoke_cli(['supervise', '--config', os.fspath(config_path)])

    assert (status, stderr) == (
        1,
        'error: worker source inventory conflicts with current assignment\n',
    )


@pytest.mark.parametrize(
    ('check', 'snapshot', 'expected'),
    [
        ('liveness', {'liveness': True, 'readiness': False}, 0),
        ('liveness', {'liveness': False, 'readiness': False}, 1),
        ('readiness', {'liveness': True, 'readiness': True}, 0),
        ('readiness', {'liveness': True, 'readiness': False}, 1),
    ],
)
def test_status_check_uses_compact_probe_exit_codes(
    capsys: pytest.CaptureFixture[str],
    check: str,
    expected: int,
    snapshot: Mapping[str, object],
    tmp_path: Path,
) -> None:
    compact = {
        **snapshot,
        'complete': True,
        'conditions': ('healthy',),
        'generated_at': 10.0,
        'snapshot_age_seconds': 0.25,
    }
    observed: list[ControlRequest] = []

    def probe(request: ControlRequest, _identity: PeerIdentity) -> Mapping[str, object]:
        observed.append(request)
        return compact

    result = _run_with_control_server(
        tmp_path,
        ['status', '--site', 'test', '--group', 'editorial', '--check', check],
        probe,
    )

    assert result == expected
    assert observed[0].command == 'probe'
    assert observed[0].arguments == {}
    assert json.loads(capsys.readouterr().out) == {
        **snapshot,
        'complete': True,
        'conditions': ['healthy'],
        'generated_at': 10.0,
        'snapshot_age_seconds': 0.25,
    }


@pytest.mark.parametrize('missing_entry', [False, True])
def test_control_audit_identity_uses_authenticated_peer(
    monkeypatch: pytest.MonkeyPatch, missing_entry: bool
) -> None:
    def lookup(uid: int) -> pwd.struct_passwd:
        assert uid == 12345
        if missing_entry:
            raise KeyError(uid)
        return pwd.struct_passwd(('service', 'x', uid, 100, '', '/', '/bin/false'))

    monkeypatch.setattr(pwd, 'getpwuid', lookup)
    peer = PeerIdentity(uid=12345, gid=100, pid=678)

    identity = cli_module._audit_identity(peer)

    assert identity.local_uid == peer.uid
    assert identity.pid == peer.pid
    assert identity.username == ('12345' if missing_entry else 'service')


def test_status_prints_structured_result(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    observed: list[ControlRequest] = []

    def status(request: ControlRequest, _identity: PeerIdentity) -> Mapping[str, object]:
        observed.append(request)
        return {
            'site': request.arguments['site'],
            'group': request.arguments['group'],
            'liveness': True,
            'readiness': True,
            'sources': [
                {
                    'site': 'test',
                    'source': 'admin',
                    'stream_incarnation': 'incarnation-1',
                }
            ],
        }

    result = _run_with_control_server(
        tmp_path,
        ['status', '--site', 'test', '--group', 'editorial'],
        status,
    )

    assert result == 0
    assert observed[0].command == 'status'
    assert observed[0].arguments == {'group': 'editorial', 'site': 'test'}
    assert json.loads(capsys.readouterr().out) == {
        'group': 'editorial',
        'liveness': True,
        'readiness': True,
        'site': 'test',
        'sources': [
            {
                'site': 'test',
                'source': 'admin',
                'stream_incarnation': 'incarnation-1',
            }
        ],
    }


@pytest.mark.parametrize('missing_entry', [False, True])
def test_supervise_recovers_locked_durable_generations_before_start(
    missing_entry: bool,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    config_path = _supervise_config(tmp_path)
    config = load_config(config_path)
    state_dir = ensure_secure_state_dir(config.daemon.state_dir)
    site_dir = ensure_secure_state_dir(ensure_secure_state_dir(state_dir / 'sites') / 'test')
    source_path = ensure_secure_state_dir(site_dir / 'sources') / 'admin.db'
    group_path = source_path
    configured = {('test', 'remote'): 'admin'}
    with _SupervisorState._open(state_dir) as supervisor_state:
        assignment = supervisor_state._reconcile_groups(
            configured=configured,
            generations={},
        )[('test', 'remote')]
    assert isinstance(assignment, ActiveSourceGroupAssignment)
    generation = assignment.group_generation
    fingerprint = stream_fingerprint(
        'https://example.invalid',
        'admin',
        'test',
    )
    with SourceState.open(
        source_path,
        writes=write_context_for(source_path),
        create=True,
        site='test',
        source='admin',
        stream_fingerprint=fingerprint,
        source_generation='source-generation-1',
    ) as source:
        source.initialize(100, incarnation='incarnation-1')
    with open_group_on_source(
        group_path,
        writes=write_context_for(group_path),
        site='test',
        site_server_url='https://example.invalid',
        source='admin',
        group='remote',
        group_generation=generation,
        ordering='eager',
        stream_fingerprint=fingerprint,
        source_generation='source-generation-1',
        verified_incarnation='incarnation-1',
        start_mode='initialize',
        initial_cursor=100,
        initial_delivery_floor=100,
        initial_last_delivered_id=100,
    ):
        pass
    with _SupervisorState._open(state_dir) as supervisor_state:
        resumed = supervisor_state._reconcile_groups(
            configured=configured,
            generations={('test', 'remote'): generation},
        )
        resumed_assignment = resumed[('test', 'remote')]
        assert isinstance(resumed_assignment, ActiveSourceGroupAssignment)
        assert resumed_assignment.start_mode == 'resume'

    started: list[dict[tuple[str, str], SourceGroupAssignment]] = []

    def record_start(supervisor: Supervisor) -> None:
        reconciled = cast(
            'dict[tuple[str, str], SourceGroupAssignment] | None',
            vars(supervisor)['_reconciled'],
        )
        assert reconciled is not None
        started.append(reconciled)

    def skip_provider_validation(_config: Config) -> None:
        pass

    def stop_without_run(_supervisor: Supervisor, **_options: object) -> None:
        pass

    monkeypatch.setattr(cli_module, '_validated_provider', skip_provider_validation)
    monkeypatch.setattr(Supervisor, 'start', record_start)
    monkeypatch.setattr(Supervisor, 'run', stop_without_run)

    if missing_entry:

        def missing_user(uid: int) -> pwd.struct_passwd:
            raise KeyError(uid)

        monkeypatch.setattr(pwd, 'getpwuid', missing_user)

    result = main(['supervise', '--config', os.fspath(config_path)])

    assert result == 0
    recovered = started[0]
    recovered_assignment = recovered[('test', 'remote')]
    assert isinstance(recovered_assignment, ActiveSourceGroupAssignment)
    assert recovered_assignment.group_generation == generation
    assert recovered_assignment.start_mode == 'resume'


def test_unavailable_stdout_is_an_error(capsys: pytest.CaptureFixture[str]) -> None:
    # Restore stdout before capsys finalizes, so its capture stream is not reinstated.
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sys, 'stdout', None)
        status = main(['--help'])

    assert (status, capsys.readouterr().err) == (2, 'error: standard output is unavailable\n')


@pytest.mark.parametrize('check', [[], ['--check', 'liveness']])
def test_unavailable_supervisor_exits_3(check: list[str], tmp_path: Path) -> None:
    status, stdout, stderr = invoke_cli(
        ['status', '--config', os.fspath(_control_config(tmp_path)), *check]
    )
    assert (status, stdout) == (3, '')
    assert stderr.startswith('error: control IPC is unavailable: ')
    assert stderr.count('\n') == 1


@pytest.mark.parametrize('command', ['status', 'validate-config', 'supervise'])
@pytest.mark.parametrize(
    ('name', 'reason'),
    [('missing.toml', 'No such file or directory'), ('.', 'Is a directory')],
)
def test_unreadable_configuration_is_a_configuration_error(
    command: str,
    name: str,
    reason: str,
    tmp_path: Path,
) -> None:
    path = tmp_path / name
    assert invoke_cli([command, '--config', os.fspath(path)]) == (
        2,
        '',
        f'error: cannot read configuration file {path}: {reason}\n',
    )


def test_unreadable_evidence_is_a_configuration_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    monkeypatch.setattr(cli_module, 'load_provider', _load_offline_provider)
    monkeypatch.setattr(cli_module, 'load_plugins', _load_no_plugins)
    path = tmp_path / 'evidence.toml'
    # The loaded credential values are redacted even when they occur in a path.
    expected_path = (
        os.fspath(path).replace('test-key', '<redacted>').replace('admin', '<redacted>')
    )

    assert invoke_cli(
        [
            'validate-config',
            '--config',
            os.fspath(_FIXTURES / 'valid.toml'),
            '--evidence',
            os.fspath(path),
        ]
    ) == (
        2,
        '',
        (
            f'{_VALIDATION_WARNING}error: cannot read acceptance evidence file {expected_path}: '
            'No such file or directory\n'
        ),
    )


def test_unrecognized_live_failure_keeps_its_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    handle = _LiveHandle(error=KeyError('records'))
    _set_live_provider(monkeypatch, handle)
    monkeypatch.setattr(cli_module, 'load_plugins', _load_no_plugins)
    _set_status_client(monkeypatch, _UnavailableStatusClient())

    with pytest.raises(KeyError, match='records'):
        invoke_cli(['validate-config', '--config', os.fspath(_FIXTURES / 'valid.toml'), '--live'])
    assert handle._closed


def test_unsafe_state_directory_exits_1(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def never_run(_supervisor: Supervisor, **_options: object) -> NoReturn:
        raise AssertionError('supervisor ran with an unsafe state directory')

    config_path = _offline_supervise_config(monkeypatch, tmp_path, never_run)
    state_dir = tmp_path / 'state'
    state_dir.mkdir(mode=0o755)
    state_dir.chmod(0o755)
    expected_path = (
        os.fspath(state_dir).replace('test-key', '<redacted>').replace('admin', '<redacted>')
    )

    status, _stdout, stderr = invoke_cli(['supervise', '--config', os.fspath(config_path)])

    assert (status, stderr) == (1, f'error: state directory must use mode 0700: {expected_path}\n')


@pytest.mark.parametrize('command', ['validate-config', 'supervise'])
@pytest.mark.parametrize(
    'message',
    [
        'selected provider shotgun-api3 is not installed',
        'both shotgun-api3 and sg-python-api own the shotgun_api3 namespace',
        'provider sg-python-api does not export AsyncShotgun',
    ],
)
def test_validate_and_supervise_report_installed_provider_failures(
    capsys: pytest.CaptureFixture[str],
    command: str,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protected_value = 'test-key'
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', protected_value)

    def fail_provider(
        _name: str,
        _required: Collection[str],
    ) -> NoReturn:
        raise ProviderError(f'{message}: {protected_value}')

    monkeypatch.setattr(
        cli_module,
        'load_provider',
        fail_provider,
    )

    result = main([command, '--config', os.fspath(_FIXTURES / 'valid.toml')])

    assert result == 2
    stderr = capsys.readouterr().err
    assert protected_value not in stderr
    assert message in stderr
    assert stderr.count('<redacted>') == 1


def test_validate_config_is_offline_and_warns_about_trusted_import_side_effects(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    provider = _OfflineProvider()
    required: list[tuple[str, frozenset[str]]] = []
    loaded_groups: list[str] = []

    def load_provider(name: str, capabilities: frozenset[str]) -> _OfflineProvider:
        required.append((name, capabilities))
        return provider

    def load_plugins(
        _site: SiteConfig,
        group: GroupConfig,
        _provider: Provider,
    ) -> tuple[()]:
        loaded_groups.append(group.name)
        return ()

    monkeypatch.setattr(cli_module, 'load_provider', load_provider)
    monkeypatch.setattr(cli_module, 'load_plugins', load_plugins)

    result = main(
        [
            'validate-config',
            '--config',
            os.fspath(_FIXTURES / 'valid.toml'),
        ]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert required == [('shotgun-api3', frozenset({'sync'}))]
    assert loaded_groups == ['noop']
    assert 'trusted plugin Python' in captured.err
    assert 'side effects' in captured.err


def test_validate_config_live_omits_incarnation_when_supervisor_is_unavailable(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    handle = _LiveHandle()
    _set_live_provider(monkeypatch, handle)
    monkeypatch.setattr(cli_module, 'load_plugins', _load_no_plugins)
    _set_status_client(monkeypatch, _UnavailableStatusClient())

    result = main(
        [
            'validate-config',
            '--config',
            os.fspath(_FIXTURES / 'valid.toml'),
            '--live',
        ]
    )

    captured = capsys.readouterr()
    evidence_path = tmp_path / 'offline-live-evidence.toml'
    evidence_path.write_text(captured.out, encoding='utf-8')
    assert result == 0
    assert load_acceptance_evidence(evidence_path).sources[0].stream_incarnation is None
    assert 'supervisor status is unavailable' in captured.err


def test_validate_config_live_prints_round_trippable_gap_evidence(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    handle = _LiveHandle(
        [
            _live_event(10),
            _live_event(9),
            _live_event(5),
            _live_event(4),
            _live_event(1),
        ]
    )

    _set_live_provider(monkeypatch, handle)
    monkeypatch.setattr(cli_module, 'load_plugins', _load_no_plugins)
    _set_status_client(
        monkeypatch,
        _StaticStatusClient(
            {
                'sources': [
                    {
                        'site': 'test',
                        'source': 'admin',
                        'stream_incarnation': 'incarnation-1',
                    }
                ]
            }
        ),
    )

    result = main(
        [
            'validate-config',
            '--config',
            os.fspath(_FIXTURES / 'valid.toml'),
            '--live',
        ]
    )

    assert result == 0
    assert len(handle._calls) == 1
    entity_type, filters, fields, options = handle._calls[0]
    assert entity_type == 'EventLogEntry'
    assert filters == []
    assert fields == list(EVENT_FIELDS)
    assert options['order'] == [{'field_name': 'id', 'direction': 'desc'}]
    assert handle._closed
    captured = capsys.readouterr()
    assert captured.out.startswith('[[sources]]\n')
    assert 'does not prove' not in captured.out
    assert 'does not prove' in captured.err
    evidence_path = tmp_path / 'live-evidence.toml'
    evidence_path.write_text(captured.out, encoding='utf-8')
    evidence = load_acceptance_evidence(evidence_path)
    assert len(evidence.sources) == 1
    record = evidence.sources[0]
    assert record.site == 'test'
    assert record.source == 'admin'
    assert record.source_fingerprint == stream_fingerprint(
        'https://example.invalid',
        'admin',
        'test',
    )
    assert record.stream_incarnation == 'incarnation-1'
    assert record.captured_at.utcoffset() is not None
    assert record.observed_gap_rate == 0.02


def test_validate_config_live_rejects_malformed_supervisor_incarnation(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    handle = _LiveHandle()
    _set_live_provider(monkeypatch, handle)
    monkeypatch.setattr(cli_module, 'load_plugins', _load_no_plugins)
    _set_status_client(
        monkeypatch,
        _StaticStatusClient(
            {
                'sources': [
                    {
                        'site': 'test',
                        'source': 'admin',
                        'stream_incarnation': 7,
                    }
                ]
            }
        ),
    )

    result = main(
        [
            'validate-config',
            '--config',
            os.fspath(_FIXTURES / 'valid.toml'),
            '--live',
        ]
    )

    captured = capsys.readouterr()
    assert result == 3
    assert captured.out == ''
    assert 'invalid source incarnation' in captured.err


def test_validate_config_live_rejects_missing_runtime_event_fields(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    incomplete = _live_event(123)
    del incomplete['meta']
    handle = _LiveHandle([incomplete])

    _set_live_provider(monkeypatch, handle)
    monkeypatch.setattr(cli_module, 'load_plugins', _load_no_plugins)
    _set_status_client(monkeypatch, _UnavailableStatusClient())

    result = main(
        [
            'validate-config',
            '--config',
            os.fspath(_FIXTURES / 'valid.toml'),
            '--live',
        ]
    )

    assert result == 1
    captured = capsys.readouterr()
    assert captured.out == ''
    assert captured.err.endswith(
        'error: live source test/<redacted> returned invalid EventLogEntry: '
        'event has missing required fields: meta\n'
    )


def test_validate_config_uses_evidence_schema_and_reports_malformed_input(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    monkeypatch.setattr(
        cli_module,
        'load_provider',
        _load_offline_provider,
    )
    monkeypatch.setattr(cli_module, 'load_plugins', _load_no_plugins)
    evidence = tmp_path / 'evidence.toml'
    evidence.write_text('sources = "not-an-array"\n', encoding='utf-8')

    result = main(
        [
            'validate-config',
            '--config',
            os.fspath(_FIXTURES / 'valid.toml'),
            '--evidence',
            os.fspath(evidence),
        ]
    )

    assert result == 2
    assert 'acceptance evidence.sources' in capsys.readouterr().err


def test_version_ignores_other_valid_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(importlib.metadata, 'version', _fixed_version)
    assert invoke_cli(['--version', *_VALID_COMMAND_ARGUMENTS]) == (
        0,
        'sg-event-daemon 9.8.7\n',
        '',
    )


def test_version_prints_the_installed_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(importlib.metadata, 'version', _fixed_version)
    assert invoke_cli(['--version']) == (0, 'sg-event-daemon 9.8.7\n', '')


def test_worker_cli_closes_control_when_partial_startup_leaves_stuck_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv('SG_EVENT_ADMIN_KEY', 'test-key')
    config_path = _supervise_config(tmp_path)
    report_path = tmp_path / 'startup-report.json'
    parent_socket, child_socket = socket.socketpair()
    control = WorkerControl(parent_socket, max_frame_bytes=8_388_608, send_deadline_secs=1.0)
    environment = dict(os.environ)
    environment['PYTHONPATH'] = os.fspath(Path(__file__).parents[1] / 'src')
    process = spawn_child(
        [
            sys.executable,
            '-c',
            _partial_startup_child_code(config_path, report_path),
            str(child_socket.fileno()),
            'partial-cli',
            '["site","test"]',
        ],
        pass_fds=(child_socket.fileno(),),
        env=environment,
    )
    child_socket.close()
    try:
        assert isinstance(control.receive(timeout=5.0), ControlHello)
        control.send(
            WorkerAssignment(
                worker_id='["site","test"]',
                spawn_instance='partial-cli',
                source_generations=(('admin', 'generation-1'),),
                group_assignments=(),
                source_group_assignments=(),
                source_endpoints=(),
                event_grants=(),
            )
        )
        with pytest.raises(EOFError):
            control.receive(timeout=5.0)
        assert json.loads(report_path.read_text()) == {
            'stuck': ['source:admin'],
            'resources_open': True,
            'monitor_started': False,
            'forced': [],
        }
        assert reap_child(process) is None
    finally:
        control.close()
        terminate_child(process, force=True)
        assert reap_child(process, timeout=5.0) is not None


def test_worker_command_is_hidden_but_callable() -> None:
    status, stdout, _stderr = invoke_cli(['--help'])
    assert status == 0
    assert 'run-worker' not in stdout
    status, stdout, _stderr = invoke_cli(['run-worker', '--help'])
    assert status == 0
    assert ' '.join(stdout.split('\n\n', 1)[0].split()) == (
        'usage: sg-event-daemon run-worker [-h] --config FILE --site SITE '
        '--worker WORKER --control-fd FD --spawn-instance ID'
    )
