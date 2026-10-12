"""Foreground supervisor, worker, validation, and operator command shell."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import pwd
import re
import signal
import sys
import threading
import time
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import TYPE_CHECKING, cast

from ._config import (
    AcceptanceEvidence,
    AcceptanceSourceEvidence,
    Config,
    ConfigError,
    evidence_warnings,
    load_acceptance_evidence,
    load_config,
    load_control_config,
    source_stream_fingerprint,
)
from ._control import (
    REQUEST_TIMEOUT_SECS,
    ControlClient,
    ControlCommandError,
    ControlProtocolError,
    ControlRequest,
    ControlServer,
    ControlUnavailableError,
)
from ._downstream import closable_stdout
from ._events import EVENT_FIELDS, Event
from ._group_types import AuditIdentity
from ._ipc import (
    ControlHello,
    OperatorCommand,
    StopCommand,
    WorkerAssignment,
    WorkerControl,
)
from ._logging import configure_logging, redact_text
from ._platform import PeerIdentity, SecurityError
from ._plugins import PluginError, load_plugins
from ._probe import ProbeServer
from ._providers import Provider, ProviderError, load_provider
from ._sqlite import DatabaseError
from ._supervisor import Supervisor
from ._worker import WorkerSpec, build_worker_runtime, build_worker_specs

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Sequence
    from types import FrameType

_CLI_ERRORS = (
    ConfigError,
    ControlCommandError,
    ControlProtocolError,
    ControlUnavailableError,
    DatabaseError,
    OSError,
    PluginError,
    ProviderError,
    RuntimeError,
    SecurityError,
    TimeoutError,
    TypeError,
    ValueError,
)
_COMMAND_GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ('run', ('supervise', 'validate-config', 'status')),
    (
        'recover a group',
        (
            'retry',
            'skip-callback',
            'restart-event',
            'park',
            'unpark',
            'replay',
            'reset-group-to-latest',
            'acknowledge-ordering-anomaly',
        ),
    ),
    ('recover a source or worker', ('adopt-source-stream', 'reset-worker-breaker')),
)
_CONTROL_CHARACTERS = re.compile(r'[\x00-\x1f\x7f-\x9f]')
_PROBE_CLEANUP_ERRORS = (OSError, RuntimeError, ValueError)
_PROVIDER_FAILURE_NAMES = frozenset(
    {'HttpLib2Error', 'ProtocolError', 'ResponseError', 'ShotgunError'}
)


class _RootParser(argparse.ArgumentParser):
    """Render options first and top-level commands under titled groups."""

    def format_help(self, formatter: argparse.HelpFormatter | None = None) -> str:
        """Return help with options first and commands in titled groups."""
        if formatter is None:
            formatter = self._get_formatter()
        formatter.add_usage(self.usage, self._actions, self._mutually_exclusive_groups)
        formatter.add_text(self.description)
        formatter.start_section('options')
        formatter.add_arguments(self._get_optional_actions())
        formatter.end_section()
        for action in self._get_positional_actions():
            commands = {
                item.dest: item
                for item in formatter._iter_indented_subactions(action)  # noqa: SLF001
            }
            for title, names in _COMMAND_GROUPS:
                formatter.start_section(title)
                formatter.add_arguments([commands[name] for name in names])
                formatter.end_section()
        formatter.add_text(self.epilog)
        return formatter.format_help()


class _RuntimeCommandError(RuntimeError):
    """Report an operator conflict or foreground runtime failure."""


class _WorkerArgumentError(ValueError):
    """Report internal worker arguments that do not match the configuration."""


def _acceptance_toml(evidence: AcceptanceEvidence) -> str:
    """Return deterministic acceptance-evidence TOML."""
    blocks: list[str] = []
    for record in evidence.sources:
        lines = [
            '[[sources]]',
            f'site = {json.dumps(record.site, ensure_ascii=True)}',
            f'source = {json.dumps(record.source, ensure_ascii=True)}',
            (f'source_fingerprint = {json.dumps(record.source_fingerprint, ensure_ascii=True)}'),
        ]
        if record.stream_incarnation is not None:
            lines.append(
                f'stream_incarnation = {json.dumps(record.stream_incarnation, ensure_ascii=True)}'
            )
        lines.extend(
            (
                f'captured_at = {record.captured_at.isoformat(timespec="seconds")}',
                f'observed_gap_rate = {record.observed_gap_rate!r}',
            )
        )
        blocks.append('\n'.join(lines))
    return '\n\n'.join(blocks) + '\n'


def _add_config_argument(parser: argparse.ArgumentParser, *, control: bool) -> None:
    """Add the required configuration path shared by every command."""
    parser.add_argument(
        '--config',
        required=True,
        type=Path,
        metavar='FILE',
        help=(
            'daemon configuration file; only its state_dir is read'
            if control
            else 'daemon configuration file'
        ),
    )


def _add_reason(parser: argparse.ArgumentParser) -> None:
    """Add one required audited reason."""
    parser.add_argument(
        '--reason',
        required=True,
        metavar='REASON',
        help='reason recorded in the audit record',
    )


def _add_target(
    parser: argparse.ArgumentParser,
    option: str,
    metavar: str,
    help_text: str,
) -> None:
    """Add one required site and one named member of that site."""
    parser.add_argument(
        '--site',
        required=True,
        metavar='SITE',
        help='site name in the configuration',
    )
    parser.add_argument(option, required=True, metavar=metavar, help=help_text)


def _assigned_worker_spec(spec: WorkerSpec, assignment: WorkerAssignment) -> WorkerSpec:
    """Narrow configured worker groups to the supervisor's lane assignment."""
    assigned = {item.group for item in assignment.group_assignments}
    if not assigned.issubset(spec.groups):
        raise ValueError('worker assignment groups conflict with configured worker')
    return replace(spec, groups=tuple(group for group in spec.groups if group in assigned))


def _audit_identity(peer: PeerIdentity) -> AuditIdentity:
    """Build trusted audit identity from the kernel-authenticated control peer."""
    try:
        username = pwd.getpwuid(peer.uid).pw_name
    except KeyError:
        username = str(peer.uid)
    return AuditIdentity(
        local_uid=peer.uid,
        username=username,
        pid=peer.pid,
    )


def _close_probe(
    probe: ProbeServer,
    *,
    shutdown_deadline_at: float | None,
    shutdown_deadline_secs: float,
) -> None:
    """Close one probe without replacing an active primary failure."""
    primary_error = sys.exception()
    cleanup_error: Exception | None = None
    try:
        probe.close()
    except _PROBE_CLEANUP_ERRORS as error:
        cleanup_error = error
    if shutdown_deadline_at is None:
        shutdown_deadline_at = time.monotonic() + shutdown_deadline_secs
    remaining = max(0.0, shutdown_deadline_at - time.monotonic())
    try:
        stopped = probe.join(timeout=remaining)
    except _PROBE_CLEANUP_ERRORS as error:
        if cleanup_error is None:
            cleanup_error = error
    else:
        if not stopped and cleanup_error is None:
            cleanup_error = _RuntimeCommandError('probe listener shutdown deadline exceeded')
    if primary_error is None and cleanup_error is not None:
        raise cleanup_error


def _control_arguments(args: argparse.Namespace) -> dict[str, object]:
    """Return the body-free arguments for one online operator command."""
    excluded = {
        'check',
        'command',
        'config',
        'control_fd',
        'evidence',
        'handler',
        'live',
        'spawn_instance',
        'version',
    }
    return {
        name: value
        for name, value in vars(args).items()
        if name not in excluded and value is not None
    }


def _describe_failure(error: Exception) -> str:
    """Return an exception's message, or its type name when the message is empty."""
    return str(error) or type(error).__name__


def _error_exit_code(error: Exception) -> int:
    """Map one anticipated shell-boundary failure to its documented exit code."""
    if isinstance(error, ControlUnavailableError | ControlProtocolError):
        return 3
    if isinstance(error, ConfigError | PluginError | ProviderError | _WorkerArgumentError):
        return 2
    return 1


@contextmanager
def _foreground_signal_handlers(
    request_stop: Callable[[str], object],
) -> Generator[None]:
    """Install process-shell SIGINT and SIGTERM handlers for one foreground run."""
    previous = {item: signal.getsignal(item) for item in (signal.SIGINT, signal.SIGTERM)}

    def stop(signum: int, _frame: FrameType | None) -> None:
        name = signal.Signals(signum).name
        request_stop(f'foreground-{name.casefold()}')

    try:
        for item in previous:
            signal.signal(item, stop)
        yield
    finally:
        for item, handler in previous.items():
            signal.signal(item, handler)


def _handle_supervisor_request(
    supervisor: Supervisor,
    request: ControlRequest,
    peer: PeerIdentity,
) -> Mapping[str, object]:
    """Translate one authenticated control request into supervisor operations."""
    if request.command == 'probe':
        return supervisor.probe_mapping()
    identity = _audit_identity(peer)
    if request.command == 'status':
        return supervisor.status_mapping(request.arguments)
    if request.command == 'reset-worker-breaker':
        return supervisor.reset_worker_breaker_command(
            request.arguments,
            identity=identity,
            wall_clock_time=time.time(),
        )
    command = OperatorCommand(
        request_id=request.request_id,
        command=request.command,
        arguments=request.arguments,
        identity=identity,
    )
    result = (
        supervisor.adopt_source_stream_command(
            command,
            timeout=REQUEST_TIMEOUT_SECS,
        )
        if request.command == 'adopt-source-stream'
        else supervisor.dispatch_group_disposition_command(
            command,
            timeout=REQUEST_TIMEOUT_SECS,
        )
        if request.command in {'replay', 'reset-group-to-latest'}
        else supervisor.dispatch_operator_command(
            command,
            timeout=REQUEST_TIMEOUT_SECS,
        )
    )
    if not result.ok:
        raise _RuntimeCommandError(result.message or 'operator command failed')
    return result.result


def _incarnations_from_status(
    result: Mapping[str, object],
) -> dict[tuple[str, str], str]:
    """Validate source status and return available non-secret incarnations."""
    raw_sources = result.get('sources')
    if not isinstance(raw_sources, list | tuple):
        raise ControlProtocolError('successful status response has invalid sources')
    source_records = cast('list[object] | tuple[object, ...]', raw_sources)
    incarnations: dict[tuple[str, str], str] = {}
    for raw_source in source_records:
        if not isinstance(raw_source, Mapping):
            raise ControlProtocolError('successful status response has invalid source health')
        source_record = cast('Mapping[str, object]', raw_source)
        site = source_record.get('site')
        source = source_record.get('source')
        if not isinstance(site, str) or not site or not isinstance(source, str) or not source:
            raise ControlProtocolError('successful status response has invalid source identity')
        if 'stream_incarnation' not in source_record:
            raise ControlProtocolError('successful status response omits source incarnation')
        incarnation = source_record['stream_incarnation']
        if incarnation is None:
            continue
        if not isinstance(incarnation, str) or not incarnation:
            raise ControlProtocolError('successful status response has invalid source incarnation')
        identity = (site, source)
        if identity in incarnations:
            raise ControlProtocolError('successful status response duplicates source health')
        incarnations[identity] = incarnation
    return incarnations


def _is_provider_failure(error: Exception) -> bool:
    """Report whether an exception is an SG provider, HTTP, or network failure."""
    return isinstance(error, OSError) or any(
        cls.__name__ in _PROVIDER_FAILURE_NAMES for cls in type(error).__mro__
    )


def _print_version() -> int:
    """Print the installed distribution version without other work."""
    try:
        version = importlib.metadata.version('sg-event-daemon')
    except importlib.metadata.PackageNotFoundError:
        print(
            'error: installed distribution metadata for sg-event-daemon is unavailable',
            file=sys.stderr,
        )
        return 2
    print(f'sg-event-daemon {version}')
    return 0


def _receive_assignment(
    control: WorkerControl,
    spec: WorkerSpec,
    spawn_instance: str,
    *,
    timeout: float,
) -> WorkerAssignment | None:
    """Receive and validate one parent-decided worker assignment."""
    try:
        message = control.receive(timeout=timeout)
    except EOFError:
        return None
    if isinstance(message, StopCommand):
        return None
    if not isinstance(message, WorkerAssignment):
        raise TypeError(f'worker received unsupported parent message: {type(message).__name__}')
    if message.worker_id != spec.worker_id or message.spawn_instance != spawn_instance:
        raise ValueError('worker assignment identity conflicts with argv')
    return message


def _report_error(message: str) -> None:
    """Print one redacted, terminal-safe error line to standard error."""
    visible = _CONTROL_CHARACTERS.sub(
        lambda match: f'\\x{ord(match.group()):02x}',
        redact_text(message),
    )
    print(f'error: {visible}', file=sys.stderr)


def _required_provider_capabilities(config: Config) -> frozenset[str]:
    """Return the provider capabilities needed by one complete configuration."""
    required = {'sync'}
    if any(
        group.execution == 'async'
        for site in config.sites.values()
        for group in site.groups.values()
    ):
        required.add('async')
    return frozenset(required)


def _run_control(args: argparse.Namespace) -> int:
    """Send one body-free request using only minimal control configuration."""
    config_path = args.config
    command = args.command
    if not isinstance(config_path, Path) or not isinstance(command, str):
        raise TypeError('control command arguments are invalid')
    control_config = load_control_config(config_path)
    check = getattr(args, 'check', None)
    request_command = 'probe' if command == 'status' and check is not None else command
    result = ControlClient.for_state_dir(control_config.state_dir).request(
        request_command,
        {} if request_command == 'probe' else _control_arguments(args),
    )
    print(
        json.dumps(
            dict(result),
            allow_nan=False,
            ensure_ascii=True,
            separators=(',', ':'),
            sort_keys=True,
        )
    )
    if check is None:
        return 0
    accepted = result.get(check)
    if type(accepted) is not bool:
        raise _RuntimeCommandError(f'status result has no boolean {check!r} field')
    return 0 if accepted else 1


def _add_control_command(command: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Add the control configuration path and bind the shared control handler."""
    _add_config_argument(command, control=True)
    command.set_defaults(handler=_run_control)
    return command


def _running_source_incarnations(config: Config) -> dict[tuple[str, str], str]:
    """Best-effort query a running supervisor for current source incarnations."""
    try:
        result = ControlClient.for_state_dir(config.daemon.state_dir).request('status')
    except ControlUnavailableError as error:
        print(f'warning: running supervisor status is unavailable: {error}', file=sys.stderr)
        return {}
    return _incarnations_from_status(result)


def _validate_plugins(config: Config, provider: Provider) -> None:
    """Import and register every trusted plugin without making an SG request."""
    print(
        'warning: validation imports trusted plugin Python and may execute side effects',
        file=sys.stderr,
    )
    for site in config.sites.values():
        for group in site.groups.values():
            load_plugins(site, group, provider)


def _validated_live_ids(
    records: object,
    *,
    label: str,
) -> tuple[int, ...]:
    """Validate one complete descending runtime EventLogEntry sample."""
    if not isinstance(records, list):
        raise TypeError(f'live source {label} returned a non-list EventLogEntry page')
    ids: list[int] = []
    for record in cast('list[object]', records):
        if not isinstance(record, Mapping):
            raise TypeError(f'live source {label} returned a non-object EventLogEntry')
        try:
            event = Event(cast('Mapping[str, object]', record))
        except ValueError as error:
            raise ValueError(
                f'live source {label} returned invalid EventLogEntry: {error}'
            ) from error
        event_id = event['id']
        if type(event_id) is not int or event_id <= 0:
            raise ValueError(f'live source {label} returned invalid EventLogEntry id')
        if event['type'] != 'EventLogEntry':
            raise ValueError(f'live source {label} returned invalid EventLogEntry type')
        ids.append(event_id)
    if any(higher <= lower for higher, lower in pairwise(ids)):
        raise ValueError(f'live source {label} returned a non-descending EventLogEntry page')
    return tuple(ids)


def _run_live_validation(config: Config, provider: Provider) -> None:
    """Make one bounded read-only EventLogEntry query for each configured source."""
    incarnations = _running_source_incarnations(config)
    evidence: list[AcceptanceSourceEvidence] = []
    for site in config.sites.values():
        for source in site.sources.values():
            credential = site.credentials[source.credentials]
            label = f'{site.name}/{source.name}'
            try:
                handle = provider.create_sync(site.server, credential)
                try:
                    records = handle.find(
                        'EventLogEntry',
                        [],
                        list(EVENT_FIELDS),
                        order=[{'field_name': 'id', 'direction': 'desc'}],
                        limit=source.batch_size,
                        page=0,
                    )
                finally:
                    handle.close()
            except Exception as error:
                if not _is_provider_failure(error):
                    raise
                raise _RuntimeCommandError(
                    f'live source {label} request failed: {_describe_failure(error)}'
                ) from error
            ids = _validated_live_ids(records, label=label)
            missing_range_count = sum(higher - lower > 1 for higher, lower in pairwise(ids))
            evidence.append(
                AcceptanceSourceEvidence(
                    site=site.name,
                    source=source.name,
                    source_fingerprint=source_stream_fingerprint(site, source),
                    stream_incarnation=incarnations.get((site.name, source.name)),
                    captured_at=datetime.now(UTC),
                    observed_gap_rate=missing_range_count / source.max_open_ranges,
                )
            )
    print(_acceptance_toml(AcceptanceEvidence(sources=tuple(evidence))), end='')
    print(
        'Live observed_gap_rate is observed missing contiguous ID ranges divided '
        'by source.max_open_ranges. Sampling does not prove server causality, '
        'retention, future visibility order, or permission completeness.',
        file=sys.stderr,
    )


def _validated_provider(config: Config) -> Provider:
    """Load the exactly selected installed provider with required capabilities."""
    return load_provider(
        config.daemon.sg_api_provider,
        _required_provider_capabilities(config),
    )


def _run_supervise(args: argparse.Namespace) -> int:
    """Run the foreground supervisor, local control endpoint, and signal shell."""
    config_path = args.config
    if not isinstance(config_path, Path):
        raise TypeError('supervisor config path is invalid')
    config = load_config(config_path)
    configure_logging(redact_from=(config,))
    _validated_provider(config)
    stop = threading.Event()
    supervisor = Supervisor(config=config, config_path=config_path)
    probe = (
        None
        if config.daemon.probe_port is None
        else ProbeServer(
            config.daemon.probe_bind,
            config.daemon.probe_port,
            supervisor.probe_result,
        )
    )
    shutdown_deadline_at: float | None = None
    try:
        with (
            _foreground_signal_handlers(lambda _reason: stop.set()),
            supervisor,
        ):
            try:
                supervisor.reconcile_durable_state()
                supervisor.start()
                if probe is not None:
                    probe.start()

                def should_stop() -> bool:
                    if probe is not None and probe.failure_reason is not None:
                        raise _RuntimeCommandError(probe.failure_reason)
                    return stop.is_set()

                with ControlServer(
                    config.daemon.state_dir,
                    lambda request, peer: _handle_supervisor_request(
                        supervisor,
                        request,
                        peer,
                    ),
                ) as control:
                    supervisor.run(
                        should_stop=should_stop,
                        wait=control.serve_once,
                        set_foreground_progress=control.set_foreground_progress,
                    )
            finally:
                shutdown_deadline_at = time.monotonic() + config.daemon.shutdown_deadline_secs
    finally:
        if probe is not None:
            _close_probe(
                probe,
                shutdown_deadline_at=shutdown_deadline_at,
                shutdown_deadline_secs=config.daemon.shutdown_deadline_secs,
            )
    return 0


def _run_validate_config(args: argparse.Namespace) -> int:
    """Validate full configuration, provider, plugins, evidence, and optional SG reads."""
    config_path = args.config
    if not isinstance(config_path, Path):
        raise TypeError('validation config path is invalid')
    config = load_config(config_path)
    configure_logging(redact_from=(config,))
    provider = _validated_provider(config)
    _validate_plugins(config, provider)
    evidence_path = args.evidence
    warnings: tuple[str, ...]
    if evidence_path is None:
        warnings = evidence_warnings(config, AcceptanceEvidence(sources=()))
    elif isinstance(evidence_path, Path):
        warnings = evidence_warnings(
            config,
            load_acceptance_evidence(evidence_path),
        )
    else:
        raise TypeError('acceptance evidence path is invalid')
    for message in warnings:
        print(f'warning: {message}', file=sys.stderr)
    if args.live:
        _run_live_validation(config, provider)
    else:
        print(
            json.dumps(
                {
                    'provider': config.daemon.sg_api_provider,
                    'sites': sorted(config.sites),
                    'validated': True,
                },
                ensure_ascii=True,
                separators=(',', ':'),
            )
        )
    return 0


def _validated_worker_args(
    args: argparse.Namespace,
) -> tuple[Path, str, str, int, str]:
    """Return one fully validated internal worker argument tuple."""
    config_path = args.config
    site = args.site
    worker_id = args.worker
    control_fd = args.control_fd
    spawn_instance = args.spawn_instance
    if not isinstance(config_path, Path):
        raise TypeError('worker config path is invalid')
    if not isinstance(site, str) or not isinstance(worker_id, str):
        raise TypeError('worker identity arguments are invalid')
    if type(control_fd) is not int or control_fd < 0:
        raise _WorkerArgumentError('control descriptor must be nonnegative')
    if not isinstance(spawn_instance, str) or not spawn_instance:
        raise _WorkerArgumentError('spawn instance must not be empty')
    return config_path, site, worker_id, control_fd, spawn_instance


def _worker_spec(config: Config, site_name: str, worker_id: str) -> WorkerSpec:
    """Return one exact configured worker assignment."""
    site = config.sites.get(site_name)
    if site is None:
        raise _WorkerArgumentError(f'unknown site: {site_name}')
    spec = next(
        (candidate for candidate in build_worker_specs(site) if candidate.worker_id == worker_id),
        None,
    )
    if spec is None:
        raise _WorkerArgumentError(f'unknown worker for site {site_name}: {worker_id}')
    return spec


def _run_worker(args: argparse.Namespace) -> int:
    """Validate identity and serve the inherited control shell."""
    config_path, site, worker_id, control_fd, spawn_instance = _validated_worker_args(args)
    config = load_config(config_path)
    spec = _worker_spec(config, site, worker_id)
    control = WorkerControl.from_fd(
        control_fd,
        max_frame_bytes=config.daemon.max_ipc_frame_bytes,
        send_deadline_secs=2 * config.daemon.health_interval_secs,
    )
    try:
        control.send(
            ControlHello(
                spawn_instance=spawn_instance,
                worker_id=spec.worker_id,
                pid=os.getpid(),
            )
        )
        assignment = _receive_assignment(
            control,
            spec,
            spawn_instance,
            timeout=config.daemon.worker_startup_deadline_secs,
        )
        if assignment is None:
            return 0
        runtime = build_worker_runtime(
            config, _assigned_worker_spec(spec, assignment), assignment, control
        )
        configure_logging(redact_from=(config, runtime))
        with _foreground_signal_handlers(runtime.request_stop):
            runtime.start()
            stuck = runtime.wait()
        if stuck:
            raise _RuntimeCommandError(f'worker shutdown deadline exceeded: {", ".join(stuck)}')
        if runtime.fatal_reason is not None:
            raise _RuntimeCommandError(runtime.fatal_reason)
        return 0
    finally:
        control.close()


def _build_parser() -> argparse.ArgumentParser:
    """Build the exact public and internal command surface."""
    parser = _RootParser(
        prog='sg-event-daemon',
        description='Deliver SG events to Python plugins with durable, recoverable progress.',
    )
    parser.add_argument('--version', action='store_true', help='show the version and exit')
    commands = parser.add_subparsers(
        dest='command',
        metavar='COMMAND',
        parser_class=argparse.ArgumentParser,
    )

    supervise = commands.add_parser(
        'supervise',
        help='run the foreground supervisor',
        description='Run the supervisor in the foreground until it is stopped.',
    )
    _add_config_argument(supervise, control=False)
    supervise.set_defaults(handler=_run_supervise)

    run_worker = commands.add_parser('run-worker')
    _add_config_argument(run_worker, control=False)
    run_worker.add_argument(
        '--site', required=True, metavar='SITE', help='site that owns the worker'
    )
    run_worker.add_argument('--worker', required=True, metavar='WORKER', help='worker ID to run')
    run_worker.add_argument(
        '--control-fd',
        required=True,
        type=int,
        metavar='FD',
        help='inherited control socket descriptor',
    )
    run_worker.add_argument(
        '--spawn-instance',
        required=True,
        metavar='ID',
        help='spawn instance assigned by the supervisor',
    )
    run_worker.set_defaults(handler=_run_worker)

    status = _add_control_command(
        commands.add_parser(
            'status',
            help='show delivery status, or check liveness or readiness',
            description="Print the running supervisor's status as JSON, or run one health check.",
        )
    )
    status.add_argument('--site', metavar='SITE', help='limit detailed records to this site')
    status.add_argument(
        '--group', metavar='GROUP', help='limit detailed records to groups with this name'
    )
    status.add_argument(
        '--check',
        choices=('liveness', 'readiness'),
        help='print a compact health result and exit 1 if this check fails',
    )

    validate_config = commands.add_parser(
        'validate-config',
        help='check the configuration and plugins without starting',
        description=(
            'Check the configuration, the installed provider, and every plugin without '
            'starting the daemon.'
        ),
    )
    _add_config_argument(validate_config, control=False)
    validate_config.add_argument(
        '--live',
        action='store_true',
        help='query each source once in SG and print acceptance evidence as TOML',
    )
    validate_config.add_argument(
        '--evidence',
        type=Path,
        metavar='FILE',
        help='compare saved acceptance evidence with the configuration',
    )
    validate_config.set_defaults(handler=_run_validate_config)

    retry = _add_control_command(
        commands.add_parser(
            'retry',
            help='retry the failed setup or callback, or refetch the event',
            description=(
                "Retry a group's failed setup or callback, or refetch the event it is waiting for."
            ),
        )
    )
    _add_target(retry, '--group', 'GROUP', 'group name in the site')

    for name, help_text, description in (
        (
            'skip-callback',
            'skip the failed callback and continue with the event',
            "Skip a group's failed callback and continue with the event's remaining callbacks.",
        ),
        (
            'restart-event',
            'run every callback of the current event again',
            (
                "Run every callback of a group's in-flight event again under the current "
                'registration.'
            ),
        ),
        (
            'park',
            'stop delivery for a group',
            "Stop a group's delivery while keeping its position and state.",
        ),
        (
            'unpark',
            'resume delivery for a parked group',
            'Resume delivery for a parked group.',
        ),
        (
            'reset-group-to-latest',
            "discard a parked group's history and start at the latest event",
            "Discard a parked group's outstanding history and move it to the latest event.",
        ),
        (
            'acknowledge-ordering-anomaly',
            'skip the late event that stopped a strict group',
            'Skip the late event that stopped a strict group, and resume with later events.',
        ),
    ):
        command = _add_control_command(
            commands.add_parser(name, help=help_text, description=description)
        )
        _add_target(command, '--group', 'GROUP', 'group name in the site')
        _add_reason(command)

    replay = _add_control_command(
        commands.add_parser(
            'replay',
            help="redeliver a parked group's events from an event ID",
            description=(
                "Redeliver a parked group's events from an event ID in a new generation."
            ),
        )
    )
    _add_target(replay, '--group', 'GROUP', 'group name in the site')
    replay.add_argument(
        '--from-event-id',
        required=True,
        type=int,
        metavar='ID',
        help='first event ID to replay, inclusive',
    )
    replay.add_argument(
        '--acknowledge-truncation',
        action='store_true',
        help="accept that events below SG's retention floor are lost",
    )
    _add_reason(replay)

    adopt = _add_control_command(
        commands.add_parser(
            'adopt-source-stream',
            help='accept a changed event stream for a source',
            description=(
                "Accept a source's changed event stream and park every group that uses it."
            ),
        )
    )
    _add_target(adopt, '--source', 'SOURCE', 'source name in the site')
    _add_reason(adopt)

    reset_breaker = _add_control_command(
        commands.add_parser(
            'reset-worker-breaker',
            help='let the supervisor restart a worker again',
            description='Reset an open worker breaker so the supervisor restarts the worker.',
        )
    )
    _add_target(
        reset_breaker, '--worker', 'WORKER', 'worker ID from the worker_id field of status'
    )
    _add_reason(reset_breaker)
    return parser


def _run(arguments: list[str]) -> int:
    """Parse one command line, dispatch it, and map anticipated failures."""
    parser = _build_parser()
    args = parser.parse_args(arguments)
    if args.version:
        return _print_version()
    if args.command is None:
        parser.error('the following arguments are required: COMMAND')
    handler = cast('Callable[[argparse.Namespace], int]', args.handler)
    try:
        return handler(args)
    except _CLI_ERRORS as error:
        _report_error(str(error))
        return _error_exit_code(error)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the sg-event-daemon command-line interface.

    If a reader closes standard output early, the remaining output is discarded
    and the exit status reflects the work completed before the closure.

    Args:
        argv: Arguments to parse, or `None` to use `sys.argv`.

    Returns:
        Zero on success, including a requested supervisor or worker stop; one
        when a command ran and failed or a health check failed; two when the
        command could not run as asked; three when the supervisor's control
        socket is unavailable or its reply is invalid; 130 when interrupted.

    Raises:
        SystemExit: With status zero for help, or two for an argument usage
            error.
    """
    if sys.stdout is None:
        # Started with standard output closed: nothing can be delivered, and the
        # closed-stdout wrapper cannot wrap a missing stream.
        _report_error('standard output is unavailable')
        return 2
    with closable_stdout():
        try:
            return _run(sys.argv[1:] if argv is None else list(argv))
        except KeyboardInterrupt:
            return 130
