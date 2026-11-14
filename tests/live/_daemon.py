from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import traceback as traceback_module
from collections.abc import Callable, Collection, Container, Iterable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType, TracebackType
from typing import IO, TYPE_CHECKING, Literal, Protocol, TextIO, TypeAlias, TypedDict, Unpack, cast
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from tests._live_mode import LiveMode, LiveProvider
    from tests.live._load import GroupDeliveryMetrics, LoadPhases
    from tests.live._site import LiveEvent, LiveSettings, LiveSite

LiveCaseFactory: TypeAlias = Callable[..., 'LiveCaseContext']


LIVE_TIMEOUT_SECS = 120.0
LIVE_SHUTDOWN_DEADLINE_SECS = 10.0
_LOG_TAIL_CHARS = 8_192
_PROBE_DIAGNOSTIC_ROWS = 10
_PROBE_KEYS = frozenset(
    {
        'attribute_name',
        'delivery_attempt',
        'entity_id',
        'entity_type',
        'event_id',
        'event_type',
        'group',
        'is_operator_replay',
        'phase_value',
    }
)
_TRACEBACK_CHARS = 4_096


@dataclass(frozen=True)
class LiveGroupSpec:
    name: str
    execution: Literal['sync', 'async']
    ordering: Literal['eager', 'strict']
    probe: Literal['sync', 'async', 'bulk']
    worker: Literal['default', 'dedicated'] = 'default'


class _LiveConfigOptions(TypedDict, total=False):
    groups: Sequence[LiveGroupSpec] | None
    late_event_window_secs: float | None
    project_id: int | None


@dataclass(frozen=True)
class ProbeRecord:
    event_id: int
    event_type: str
    attribute_name: str | None
    entity_type: str
    entity_id: int
    delivery_attempt: int
    is_operator_replay: bool
    group: str
    phase_value: str | None
    delivered_at: float | None = None


@dataclass
class ProducerProgress:
    phase: Literal['live', 'stopped']
    requested_updates: int
    successful_updates: int = 0
    phase_values: list[str] = field(default_factory=list[str])
    elapsed_secs: float = 0.0


@dataclass
class LiveReport:
    site_hostname: str
    project_name: str
    provider: LiveProvider
    mode: LiveMode
    namespace: str
    expected_phases: list[str] = field(default_factory=list[str])
    expected_ids: list[int] = field(default_factory=list[int])
    source_visible_count: int = 0
    requested_updates: int = 0
    successful_updates: int = 0
    producer_progress: dict[str, ProducerProgress] = field(
        default_factory=dict[str, ProducerProgress]
    )
    achieved_rate: float | None = None
    load_metrics: dict[str, dict[str, object]] = field(
        default_factory=dict[str, dict[str, object]]
    )
    group_records: dict[str, list[ProbeRecord]] = field(
        default_factory=dict[str, list[ProbeRecord]]
    )
    timings: dict[str, float] = field(default_factory=dict[str, float])
    status_summary: dict[str, object] = field(default_factory=dict[str, object])
    cleanup_mode: str = 'producer'
    cleanup_outcome: list[str] = field(default_factory=list[str])
    failure_categories: list[str] = field(default_factory=list[str])

    def record_load(
        self,
        phases: LoadPhases,
        metrics: Mapping[str, GroupDeliveryMetrics],
        *,
        final_status: Mapping[str, object],
        live_convergence_secs: float,
        restart_convergence_secs: float,
    ) -> None:
        live_duration = self.producer_progress.get(
            'live', ProducerProgress('live', 0)
        ).elapsed_secs
        stopped_duration = self.producer_progress.get(
            'stopped', ProducerProgress('stopped', 0)
        ).elapsed_secs
        producer_duration = sum(
            progress.elapsed_secs
            for progress in self.producer_progress.values()
            if progress.elapsed_secs > 0
        )
        self.timings.update(
            {
                'live_convergence_secs': live_convergence_secs,
                'live_producer_secs': live_duration,
                'producer_secs': producer_duration,
                'restart_convergence_secs': restart_convergence_secs,
                'stopped_producer_secs': stopped_duration,
            }
        )
        self.status_summary = dict(final_status)
        self.achieved_rate = (
            None if producer_duration == 0 else self.successful_updates / producer_duration
        )
        self.load_metrics = {
            'phases': {
                'live_count': phases.live_count,
                'stopped_count': phases.stopped_count,
            },
            **{
                group: {
                    'delivered_unique_count': len(evidence.delivered_unique_ids),
                    'delivered_unique_ids': evidence.delivered_unique_ids,
                    'duplicate_count': len(evidence.duplicate_ids),
                    'duplicate_ids': evidence.duplicate_ids,
                    'inversion_count': len(evidence.inversions),
                    'inversions': evidence.inversions,
                    'missing_count': len(evidence.missing_ids),
                    'missing_ids': evidence.missing_ids,
                }
                for group, evidence in metrics.items()
            },
        }


class SanitizedLiveError(Exception):
    def __init__(
        self,
        category: str,
        original: BaseException,
        secrets: Iterable[str] = (),
    ) -> None:
        self.category = category
        self.original = original
        rendered = f'{type(original).__name__}: {original}'
        formatted = ''.join(
            traceback_module.format_exception(type(original), original, original.__traceback__)
        )
        for secret in secrets:
            if secret:
                rendered = rendered.replace(secret, '<redacted>')
                formatted = formatted.replace(secret, '<redacted>')
        self.traceback_text = formatted[-_TRACEBACK_CHARS:]
        super().__init__(f'{category}: {rendered[-_TRACEBACK_CHARS:]}')

    def __repr__(self) -> str:
        return f'{type(self).__name__}({str(self)!r})'


class SupervisorProcess:
    def __init__(
        self,
        *,
        config_path: Path,
        environment: Mapping[str, str],
        directory: Path,
        timeout_secs: float,
        command: tuple[str, ...] | None = None,
    ) -> None:
        if command == ():
            raise ValueError('supervisor command must not be empty')
        if timeout_secs < 0:
            raise ValueError('supervisor timeout must not be negative')
        self._command = command
        self._config_path = config_path
        self._directory = directory
        self._environment = dict(environment)
        self._generation = 0
        self._last_status_result: Mapping[str, object] | None = None
        self._output_log_paths: tuple[Path, Path] = (
            directory / 'not-started.out',
            directory / 'not-started.err',
        )
        self.pgid: int | None = None
        self.process: subprocess.Popen[bytes] | None = None
        self.stderr: IO[bytes] | None = None
        self.stdout: IO[bytes] | None = None
        self._stop_failures: tuple[BaseException, ...] | None = None
        self.timeout_secs = timeout_secs

    def _command_value(self) -> tuple[str, ...]:
        if self._command is not None:
            return self._command
        return (
            sys.executable,
            '-m',
            'sg_event_daemon',
            'supervise',
            '--config',
            os.fspath(self._config_path),
        )

    def _reap_with_timeout(self, process: subprocess.Popen[bytes]) -> bool:
        try:
            process.wait(timeout=self.timeout_secs)
        except subprocess.TimeoutExpired:
            return False
        return True

    def _shutdown_failure(self, error: BaseException) -> BaseException:
        if isinstance(error, (KeyboardInterrupt, GeneratorExit, asyncio.CancelledError)):
            return error
        return SanitizedLiveError('shutdown', error)

    def _kill_direct_child(
        self,
        process: subprocess.Popen[bytes],
        failures: list[BaseException],
    ) -> None:
        if process.poll() is not None:
            return
        try:
            process.kill()
        except ProcessLookupError:
            pass
        except BaseException as error:  # noqa: BLE001
            failures.append(self._shutdown_failure(error))

    def _status_command(
        self,
        check: Literal['liveness', 'readiness'] | None,
        group: str | None,
    ) -> tuple[str, ...]:
        command = [
            sys.executable,
            '-m',
            'sg_event_daemon',
            'status',
            '--config',
            os.fspath(self._config_path),
        ]
        if check is not None:
            command.extend(('--check', check))
        if group is not None:
            command.extend(('--group', group))
        return tuple(command)

    def _unsafe_group(self, pgid: int) -> bool:
        process = self.process
        return (
            process is None
            or pgid in {0, 1, os.getpgrp()}
            or process.pid in {0, 1, os.getpid()}
            or pgid != process.pid
        )

    def _kill_validated_group(
        self,
        validated_pgid: int | None,
        failures: list[BaseException],
    ) -> None:
        if validated_pgid is None:
            return
        if self._unsafe_group(validated_pgid):
            failures.append(
                SanitizedLiveError('shutdown', RuntimeError('refused unsafe forced shutdown'))
            )
            return
        try:
            os.killpg(validated_pgid, signal.SIGKILL)
        except BaseException as error:  # noqa: BLE001
            failures.append(self._shutdown_failure(error))

    def close_logs(self) -> None:
        first_error: BaseException | None = None
        for handle in (self.stdout, self.stderr):
            if handle is not None and not handle.closed:
                try:
                    handle.close()
                except BaseException as error:  # noqa: BLE001
                    if first_error is None:
                        first_error = error
                    else:
                        first_error.add_note(f'additional log cleanup failure: {error!r}')
        self.stdout = None
        self.stderr = None
        if first_error is not None:
            raise first_error

    @property
    def last_status(self) -> Mapping[str, object] | None:
        return self._last_status_result

    @property
    def log_paths(self) -> tuple[Path, Path]:
        return self._output_log_paths

    @property
    def pid(self) -> int | None:
        return None if self.process is None else self.process.pid

    def sanitize_text(self, value: str) -> str:
        secret = self._environment.get('SG_EVENT_TEST_SOURCE_KEY')
        if secret:
            value = value.replace(secret, '<redacted>')
        return value[-_LOG_TAIL_CHARS:]

    def _sanitize_status(self, value: object) -> object:
        if isinstance(value, str):
            return self.sanitize_text(value)
        if isinstance(value, Mapping):
            mapping = cast('Mapping[object, object]', value)
            return {
                key: self._sanitize_status(item)
                for key, item in mapping.items()
                if isinstance(key, str)
            }
        if isinstance(value, (list, tuple)):
            return tuple(self._sanitize_status(item) for item in cast('Sequence[object]', value))
        return value

    def read_log_tail(self, path: Path) -> str:
        try:
            with path.open('rb') as handle:
                handle.seek(0, os.SEEK_END)
                handle.seek(max(0, handle.tell() - (_LOG_TAIL_CHARS * 4)))
                text = handle.read().decode('utf-8', errors='replace')
        except OSError as error:
            text = f'<unavailable:{type(error).__name__}>'
        return self.sanitize_text(text)

    def status(
        self,
        *,
        check: Literal['liveness', 'readiness'] | None = None,
        group: str | None = None,
    ) -> Mapping[str, object]:
        result = subprocess.run(
            self._status_command(check, group),
            capture_output=True,
            check=False,
            cwd=self._directory,
            env=self._environment,
            text=True,
            timeout=min(LIVE_TIMEOUT_SECS, 10.0),
        )
        try:
            decoded = json.loads(result.stdout)
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, dict) and result.returncode in {0, 1}:
            status = cast('dict[str, object]', decoded)
            sanitized = self._sanitize_status(status)
            self._last_status_result = MappingProxyType(cast('dict[str, object]', sanitized))
            return self._last_status_result
        stdout_tail = self.sanitize_text(result.stdout)
        stderr_tail = self.sanitize_text(result.stderr)
        raise RuntimeError(
            f'status command failed ({result.returncode}); '
            f'stdout_tail={stdout_tail!r}; stderr_tail={stderr_tail!r}'
        )

    def terminate_and_reap(
        self,
        process: subprocess.Popen[bytes],
        *,
        validated_pgid: int | None,
    ) -> tuple[BaseException, ...]:
        failures: list[BaseException] = []
        if process.poll() is not None:
            process.wait()
            return ()
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        except BaseException as error:  # noqa: BLE001
            failures.append(self._shutdown_failure(error))
        if self._reap_with_timeout(process):
            return tuple(failures)
        failures.append(
            SanitizedLiveError(
                'shutdown',
                RuntimeError(f'graceful shutdown timed out after {self.timeout_secs}s'),
            )
        )
        self._kill_validated_group(validated_pgid, failures)
        self._kill_direct_child(process, failures)
        if not self._reap_with_timeout(process):
            failures.append(
                SanitizedLiveError(
                    'shutdown', RuntimeError('direct child did not exit after forced shutdown')
                )
            )
        return tuple(failures)

    def start(self) -> None:
        if self.process is not None and self.process.poll() is None:
            raise RuntimeError('supervisor process is already running')
        self.close_logs()
        self._directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._generation += 1
        self._output_log_paths = (
            self._directory / f'daemon-{self._generation}.stdout.log',
            self._directory / f'daemon-{self._generation}.stderr.log',
        )
        self.stdout = self._output_log_paths[0].open('ab', buffering=0)
        self.stderr = self._output_log_paths[1].open('ab', buffering=0)
        self._stop_failures = None
        process = subprocess.Popen(
            self._command_value(),
            cwd=self._directory,
            env=self._environment,
            start_new_session=True,
            stderr=self.stderr,
            stdout=self.stdout,
        )
        self.process = process
        try:
            pgid = os.getpgid(process.pid)
        except ProcessLookupError:
            try:
                self.terminate_and_reap(process, validated_pgid=None)
            finally:
                self.close_logs()
            raise RuntimeError('supervisor process group could not be resolved') from None
        if self._unsafe_group(pgid):
            try:
                self.terminate_and_reap(process, validated_pgid=None)
            finally:
                self.close_logs()
            raise RuntimeError('supervisor child has an unsafe process group')
        self.pgid = pgid

    def stop(self) -> tuple[BaseException, ...]:
        if self._stop_failures is not None:
            return self._stop_failures
        process = self.process
        if process is None:
            self._stop_failures = ()
            return ()
        initiating: BaseException | None = None
        try:
            failures = self.terminate_and_reap(process, validated_pgid=self.pgid)
        except BaseException as error:
            initiating = error
            raise
        finally:
            try:
                self.close_logs()
            except BaseException as error:
                if initiating is None:
                    raise
                initiating.add_note(f'log cleanup failure: {error!r}')
            finally:
                self.pgid = None
        self._stop_failures = failures
        return self._stop_failures

    def wait_ready(self) -> Mapping[str, object]:
        deadline = time.monotonic() + LIVE_TIMEOUT_SECS
        last_failure = 'no status response'
        while True:
            process = self.process
            if process is None or process.poll() is not None:
                raise RuntimeError('supervisor exited before readiness')
            try:
                status = self.status()
                if status.get('liveness') is True and status.get('readiness') is True:
                    return status
                last_failure = 'readiness remained false'
            except RuntimeError as error:
                last_failure = str(error)[-_LOG_TAIL_CHARS:]
            if time.monotonic() >= deadline:
                raise RuntimeError(f'supervisor readiness timed out: {last_failure}')
            time.sleep(0.05)


class _ProbeReader:
    def __init__(self, path: Path, *, expected_group: str | None = None) -> None:
        self._path = path
        self._expected_group = expected_group
        self._offset = 0
        self._pending = b''
        self._records: list[ProbeRecord] = []
        self._groups: set[str] = set()

    def read(self) -> tuple[ProbeRecord, ...]:
        if not self._path.exists():
            return tuple(self._records)
        with self._path.open('rb') as stream:
            stream.seek(self._offset)
            data = self._pending + stream.read()
            self._offset = stream.tell()
        lines = data.split(b'\n')
        self._pending = lines.pop()
        for line in lines:
            line_number = len(self._records) + 1
            try:
                value = json.loads(line.decode('utf-8'))
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise ValueError(f'probe row {line_number} is invalid JSON') from None
            record = _probe_record(value, line_number)
            self._groups.add(record.group)
            if len(self._groups) > 1:
                raise ValueError('probe file contains mixed group identities')
            if self._expected_group is not None and record.group != self._expected_group:
                raise ValueError('probe file contains an unexpected group identity')
            self._records.append(record)
        return tuple(self._records)


@dataclass
class LiveCase:
    settings: LiveSettings
    site: LiveSite
    process: SupervisorProcess
    config_path: Path
    baseline_event: LiveEvent
    output_paths: Mapping[str, Path]
    report: LiveReport
    probe_readers: dict[str, _ProbeReader] = field(default_factory=dict[str, '_ProbeReader'])


class ContextHooks(Protocol):
    def build_process(self, config_path: Path) -> SupervisorProcess: ...
    def cleanup(self, site: LiveSite) -> tuple[tuple[str, BaseException], ...]: ...
    def cleanup_clients(self, clients: object) -> tuple[tuple[str, BaseException], ...]: ...
    def create_shot(self, site: LiveSite) -> int: ...
    def discover_baseline(self, site: LiveSite, value: str) -> LiveEvent: ...
    def open_clients(self) -> object: ...
    def resolve_site(self, clients: object) -> LiveSite: ...
    def validate_child_import(self) -> None: ...
    def validate_provider(self) -> None: ...
    def write_baseline(self, site: LiveSite) -> str: ...
    def write_config(
        self,
        site: LiveSite,
        shot_id: int,
    ) -> tuple[Path, Mapping[str, Path]]: ...
    def write_report(
        self,
        report: LiveReport,
        failures: tuple[BaseException, ...],
    ) -> None: ...


class OpenClientsError(Exception):
    def __init__(
        self,
        original: BaseException,
        cleanup_failures: tuple[tuple[str, BaseException], ...],
    ) -> None:
        self.cleanup_failures = cleanup_failures
        self.original = original
        super().__init__('provider client setup failed')


class LiveCaseContext(AbstractContextManager[LiveCase]):
    def __init__(
        self,
        *,
        settings: LiveSettings,
        directory: Path,
        hooks: ContextHooks,
    ) -> None:
        self._case: LiveCase | None = None
        self._clients: object | None = None
        self._directory = directory
        self._failures: tuple[BaseException, ...] | None = None
        self._hooks = hooks
        self._settings = settings
        hostname = urlsplit(settings.site).hostname
        if hostname is None:
            raise ValueError('live settings site must include a hostname')
        self._report = LiveReport(
            site_hostname=hostname,
            project_name=settings.project,
            provider=settings.options.provider,
            mode=settings.options.mode,
            namespace='',
            cleanup_mode='quiet' if settings.cleanup is not None else 'producer',
        )
        self._site: LiveSite | None = None

    def __enter__(self) -> LiveCase:
        try:
            self._hooks.validate_provider()
            self._hooks.validate_child_import()
            clients = self._hooks.open_clients()
            self._clients = clients
            site = self._hooks.resolve_site(clients)
            self._site = site
            self._clients = None
            self._report.namespace = site.namespace
            shot_id = self._hooks.create_shot(site)
            baseline_value = self._hooks.write_baseline(site)
            baseline_event = self._hooks.discover_baseline(site, baseline_value)
            config_path, output_paths = self._hooks.write_config(site, shot_id)
            process = self._hooks.build_process(config_path)
            self._case = LiveCase(
                settings=self._settings,
                site=site,
                process=process,
                config_path=config_path,
                baseline_event=baseline_event,
                output_paths=MappingProxyType(dict(output_paths)),
                report=self._report,
            )
        except BaseException as error:  # noqa: BLE001
            if isinstance(error, OpenClientsError):
                failures = [self._sanitize('setup', error.original)]
                failures.extend(
                    self._sanitize(category, failure)
                    for category, failure in error.cleanup_failures
                )
            else:
                failures = [self._sanitize('setup', error)]
            failures.extend(self._finalize_components(include_process=False))
            self._write_report(failures)
            self._failures = tuple(failures)
            raise BaseExceptionGroup('live acceptance setup', failures) from None
        else:
            return self._case

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        del exception_type, traceback
        failures: list[BaseException] = []
        if exception is not None:
            failures.append(self._sanitize('test', exception))
        failures.extend(self._finalize_components(include_process=True))
        self._write_report(failures)
        self._failures = tuple(failures)
        if failures:
            raise BaseExceptionGroup('live acceptance failures', failures) from None
        return False

    def _secrets(self) -> tuple[str, ...]:
        values = [self._settings.source.api_key, self._settings.producer.api_key]
        if self._settings.cleanup is not None:
            values.append(self._settings.cleanup.api_key)
        return tuple(values)

    def _sanitize(self, category: str, error: BaseException) -> BaseException:
        if isinstance(error, (KeyboardInterrupt, GeneratorExit)):
            return error
        if isinstance(error, asyncio.CancelledError):
            return error
        if isinstance(error, SanitizedLiveError):
            if error.category == category:
                return error
            return SanitizedLiveError(category, error.original, self._secrets())
        return SanitizedLiveError(category, error, self._secrets())

    def _finalize_cleanup(self) -> tuple[list[BaseException], str]:
        failures: list[BaseException] = []
        site = self._site
        if site is not None:
            try:
                cleanup_failures = self._hooks.cleanup(site)
            except BaseException as error:  # noqa: BLE001
                return [self._sanitize('cleanup', error)], 'cleanup:failed'
            failures.extend(
                self._sanitize(category, error) for category, error in cleanup_failures
            )
            outcome = 'cleanup:failed' if cleanup_failures else 'cleanup:succeeded'
            return failures, outcome
        clients = self._clients
        if clients is None:
            return failures, 'cleanup:not-required'
        try:
            cleanup_failures = self._hooks.cleanup_clients(clients)
        except BaseException as error:  # noqa: BLE001
            failures.append(self._sanitize('cleanup', error))
            outcome = 'client-cleanup:failed'
        else:
            failures.extend(
                self._sanitize(category, error) for category, error in cleanup_failures
            )
            outcome = 'client-cleanup:failed' if cleanup_failures else 'client-cleanup:succeeded'
        self._clients = None
        return failures, outcome

    def _finalize_process(
        self,
        *,
        include_process: bool,
    ) -> tuple[list[BaseException], str | None]:
        case = self._case
        if not include_process or case is None:
            return [], None
        try:
            process_failures = case.process.stop()
        except BaseException as error:  # noqa: BLE001
            return [self._sanitize('shutdown', error)], 'shutdown:failed'
        failures = [self._sanitize('shutdown', failure) for failure in process_failures]
        outcome = 'shutdown:failed' if process_failures else 'shutdown:succeeded'
        return failures, outcome

    def _finalize_components(self, *, include_process: bool) -> list[BaseException]:
        process_failures, process_outcome = self._finalize_process(include_process=include_process)
        cleanup_failures, cleanup_outcome = self._finalize_cleanup()
        outcomes = [cleanup_outcome]
        if process_outcome is not None:
            outcomes.insert(0, process_outcome)
        self._report.cleanup_outcome = outcomes
        return [*process_failures, *cleanup_failures]

    def _update_report(self, failures: Sequence[BaseException]) -> None:
        site = self._site
        baseline_id = None if self._case is None else self._case.baseline_event.id
        if site is not None:
            observed = tuple(event for event in site.observed_events if event.id != baseline_id)
            self._report.expected_ids = [event.id for event in observed]
            self._report.source_visible_count = len(observed)
        self._report.failure_categories = [
            failure.category for failure in failures if isinstance(failure, SanitizedLiveError)
        ]

    def _write_report(self, failures: list[BaseException]) -> None:
        self._update_report(failures)
        try:
            self._hooks.write_report(self._report, tuple(failures))
        except BaseException as error:  # noqa: BLE001
            failures.append(self._sanitize('report', error))
            self._update_report(failures)

    def finalize(self) -> tuple[BaseException, ...]:
        if self._failures is not None:
            return self._failures
        failures = self._finalize_components(include_process=True)
        self._write_report(failures)
        self._failures = tuple(failures)
        return self._failures


def _probe_record(value: object, line_number: int) -> ProbeRecord:
    if not isinstance(value, dict):
        raise TypeError(f'probe row {line_number} must be an object')
    row = cast('dict[str, object]', value)
    if frozenset(row) not in (_PROBE_KEYS, _PROBE_KEYS | {'delivered_at'}):
        raise ValueError(f'probe row {line_number} has invalid keys')
    delivery_attempt = row['delivery_attempt']
    entity_id = row['entity_id']
    event_id = row['event_id']
    if any(type(item) is not int for item in (delivery_attempt, entity_id, event_id)):
        raise ValueError(f'probe row {line_number} has invalid integer fields')
    typed_delivery_attempt = cast('int', delivery_attempt)
    typed_entity_id = cast('int', entity_id)
    typed_event_id = cast('int', event_id)
    if typed_delivery_attempt <= 0 or typed_event_id <= 0:
        raise ValueError(f'probe row {line_number} has nonpositive integer fields')
    attribute_name = row['attribute_name']
    entity_type = row['entity_type']
    event_type = row['event_type']
    group = row['group']
    if any(not isinstance(item, str) or not item for item in (entity_type, event_type, group)):
        raise ValueError(f'probe row {line_number} has invalid text fields')
    if attribute_name is not None and (not isinstance(attribute_name, str) or not attribute_name):
        raise ValueError(f'probe row {line_number} has invalid attribute name')
    delivered_at = row.get('delivered_at')
    if delivered_at is not None and type(delivered_at) is not float:
        raise ValueError(f'probe row {line_number} has invalid delivery time')
    replay = row['is_operator_replay']
    if type(replay) is not bool:
        raise ValueError(f'probe row {line_number} has invalid replay flag')
    phase = row['phase_value']
    if phase is not None and not isinstance(phase, str):
        raise ValueError(f'probe row {line_number} has invalid phase evidence')
    return ProbeRecord(
        event_id=typed_event_id,
        event_type=cast('str', event_type),
        attribute_name=attribute_name,
        entity_type=cast('str', entity_type),
        entity_id=typed_entity_id,
        delivery_attempt=typed_delivery_attempt,
        is_operator_replay=replay,
        group=cast('str', group),
        phase_value=phase,
        delivered_at=delivered_at,
    )


def _probe_timeout_diagnostic(
    live_case: LiveCase,
    expected_ids: Collection[int],
    records_by_group: Mapping[str, Sequence[ProbeRecord]],
) -> str:
    log_tails = [live_case.process.read_log_tail(path) for path in live_case.process.log_paths]
    last_status = live_case.process.last_status
    diagnostic = {
        'daemon_stderr_tail': log_tails[1],
        'daemon_stdout_tail': log_tails[0],
        'expected_event_ids': sorted(set(expected_ids)),
        'last_group_callback_rows': {
            group: [asdict(record) for record in records[-_PROBE_DIAGNOSTIC_ROWS:]]
            for group, records in records_by_group.items()
        },
        'last_status': None if last_status is None else dict(last_status),
    }
    return json.dumps(diagnostic, ensure_ascii=True, separators=(',', ':'))


def _toml_string(value: str | Path) -> str:
    return json.dumps(os.fspath(value), ensure_ascii=True)


def _validated_group_statuses(
    status: Mapping[str, object],
    expected_groups: Iterable[str],
) -> tuple[Mapping[str, object], ...]:
    groups = status.get('groups')
    if not isinstance(groups, (list, tuple)) or not groups:
        raise AssertionError('final supervisor status has no group progress')
    expected_identities = tuple(expected_groups)
    if not expected_identities or len(expected_identities) != len(set(expected_identities)):
        raise ValueError('expected group identities must be nonempty and unique')
    observed_groups: list[Mapping[str, object]] = []
    for raw_group in cast('Sequence[object]', groups):
        if not isinstance(raw_group, Mapping):
            raise TypeError('final group status is malformed')
        group = cast('Mapping[str, object]', raw_group)
        identity = group.get('group')
        if not isinstance(identity, str) or not identity:
            raise TypeError('final group identity is malformed')
        observed_groups.append(group)
    observed_identities = [cast('str', group['group']) for group in observed_groups]
    if len(observed_identities) != len(set(observed_identities)):
        raise AssertionError('final group identities are not unique')
    if set(observed_identities) != set(expected_identities):
        raise AssertionError('final group identity coverage differs from configured groups')
    return tuple(observed_groups)


def assert_group_cursors(
    status: Mapping[str, object],
    *,
    expected_groups: Iterable[str],
    expected_floor: int,
) -> None:
    if status.get('liveness') is not True or status.get('readiness') is not True:
        raise AssertionError('final supervisor status is not live and ready')
    primary_state = status.get('primary_state')
    if primary_state not in {'healthy', 'idle', 'parked'}:
        raise AssertionError('final supervisor status is not readiness-compatible')
    conditions = status.get('conditions')
    if not isinstance(conditions, (list, tuple)):
        raise TypeError('final supervisor conditions are malformed')
    for group in _validated_group_statuses(status, expected_groups):
        cursor = group.get('cursor')
        if type(cursor) is not int or cursor < expected_floor:
            raise AssertionError('final group cursor is below the expected floor')
        if group.get('status') not in {'running', 'parked'}:
            raise AssertionError('final group status retains a fatal condition')
        if group.get('failure_kind') is not None:
            raise AssertionError('final group status retains a fatal failure')


def build_child_environment(
    settings: LiveSettings,
    environ: Mapping[str, str],
    source_root: Path,
) -> dict[str, str]:
    environment = dict(environ)
    existing = environment.get('PYTHONPATH')
    environment['PYTHONPATH'] = (
        os.fspath(source_root.resolve())
        if not existing
        else os.pathsep.join((os.fspath(source_root.resolve()), existing))
    )
    if environment.get('SG_EVENT_TEST_REQUEST_LOG'):
        recorder = Path(__file__).with_name('fixtures') / 'request_recorder'
        environment['PYTHONPATH'] = os.pathsep.join(
            (os.fspath(recorder.resolve()), environment['PYTHONPATH'])
        )
    environment['SG_EVENT_TEST_SOURCE_KEY'] = settings.source.api_key
    for name in tuple(environment):
        if name in {'SG_EVENT_TEST_CLEANUP_KEY', 'SG_EVENT_TEST_PRODUCER_KEY'}:
            environment.pop(name)
    return environment


def event_ids(events: Iterable[LiveEvent]) -> tuple[int, ...]:
    return tuple(event.id for event in events)


def read_probe_records(
    path: Path,
    *,
    expected_group: str | None = None,
    reader: _ProbeReader | None = None,
) -> tuple[ProbeRecord, ...]:
    return (reader or _ProbeReader(path, expected_group=expected_group)).read()


def read_all_groups(live_case: LiveCase) -> Mapping[str, tuple[ProbeRecord, ...]]:
    for group, path in live_case.output_paths.items():
        if group not in live_case.probe_readers:
            live_case.probe_readers[group] = _ProbeReader(path, expected_group=group)
    return MappingProxyType(
        {
            group: read_probe_records(
                path, expected_group=group, reader=live_case.probe_readers[group]
            )
            for group, path in live_case.output_paths.items()
        }
    )


def assert_group_deliveries(
    live_case: LiveCase,
    expected_events: Collection[LiveEvent],
    *,
    excluded_ids: Container[int] = (),
) -> Mapping[str, tuple[ProbeRecord, ...]]:
    records_by_group = read_all_groups(live_case)
    expected = event_ids(expected_events)
    sequences: list[tuple[int, ...]] = []
    for records in records_by_group.values():
        ids = tuple(record.event_id for record in records)
        if any(identifier in excluded_ids for identifier in ids):
            raise AssertionError('excluded event reached a probe group')
        if ids != expected or len(ids) != len(set(ids)):
            raise AssertionError('probe delivery is not exact and ordered')
        for record, event in zip(records, expected_events, strict=True):
            if record.phase_value != event.phase_value:
                raise AssertionError('probe phase evidence differs from source evidence')
        sequences.append(ids)
    if sequences and len(set(sequences)) != 1:
        raise AssertionError('probe groups observed different event sequences')
    status = live_case.process.status()
    if status.get('liveness') is not True or status.get('readiness') is not True:
        raise AssertionError('supervisor is not live and ready after delivery')
    live_case.report.group_records = {
        group: list(records) for group, records in records_by_group.items()
    }
    live_case.report.status_summary = dict(status)
    return records_by_group


def validate_child_import(environment: Mapping[str, str], directory: Path) -> None:
    result = subprocess.run(
        (sys.executable, '-c', 'import sg_event_daemon'),
        capture_output=True,
        check=False,
        cwd=directory,
        env=dict(environment),
        text=True,
        timeout=10.0,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f'child import validation failed ({result.returncode}): '
            f'{result.stderr[-_LOG_TAIL_CHARS:]}'
        )


def wait_for_group_event_set(
    live_case: LiveCase,
    expected_events: Iterable[LiveEvent],
) -> Mapping[str, tuple[ProbeRecord, ...]]:
    expected_by_id = {event.id: event for event in expected_events}
    deadline = time.monotonic() + LIVE_TIMEOUT_SECS
    while True:
        records_by_group = read_all_groups(live_case)
        if records_by_group and all(
            {record.event_id for record in records} == set(expected_by_id)
            and all(
                record.phase_value == expected_by_id[record.event_id].phase_value
                for record in records
            )
            for records in records_by_group.values()
        ):
            return records_by_group
        if time.monotonic() >= deadline:
            diagnostic = _probe_timeout_diagnostic(
                live_case,
                expected_by_id,
                records_by_group,
            )
            raise AssertionError(
                'probe groups did not converge on the expected event set; '
                f'diagnostics={diagnostic}'
            )
        time.sleep(0.05)


def wait_for_group_records(
    live_case: LiveCase,
    *,
    expected_events: Collection[LiveEvent],
) -> Mapping[str, tuple[ProbeRecord, ...]]:
    expected = event_ids(expected_events)
    deadline = time.monotonic() + LIVE_TIMEOUT_SECS
    while True:
        records_by_group = read_all_groups(live_case)
        if records_by_group and all(
            tuple(record.event_id for record in records) == expected
            and all(
                record.phase_value == event.phase_value
                for record, event in zip(records, expected_events, strict=True)
            )
            for records in records_by_group.values()
        ):
            return records_by_group
        if time.monotonic() >= deadline:
            diagnostic = _probe_timeout_diagnostic(live_case, expected, records_by_group)
            raise AssertionError(
                f'probe groups did not converge on ordered events; diagnostics={diagnostic}'
            )
        time.sleep(0.05)


def write_daemon_config(
    paths: Mapping[str, Path],
    settings: LiveSettings,
    *,
    shot_id: int,
    production_requests: bool = False,
    **options: Unpack[_LiveConfigOptions],
) -> Path:
    groups = options.get('groups')
    late_event_window_secs = options.get('late_event_window_secs')
    project_id = options.get('project_id')
    config_path = paths['config'].resolve()
    state_path = paths['state'].resolve()
    state_path.mkdir(parents=True, exist_ok=True, mode=0o700)
    state_path.chmod(0o700)
    group_names = ('sync',) if settings.options.mode == 'sync' else ('async', 'sync')
    fixture_directory = Path(__file__).with_name('fixtures').resolve()
    blocks = [
        '[daemon]',
        f'state_dir = {_toml_string(state_path)}',
        f'sg_api_provider = {_toml_string(settings.options.provider)}',
        'supervisor_poll_interval_secs = 0.05',
        f'shutdown_deadline_secs = {LIVE_SHUTDOWN_DEADLINE_SECS}',
        '',
        '[sites.live]',
        f'server = {_toml_string(settings.site)}',
        'default_source = "events"',
        '',
        '[sites.live.credentials.source]',
        f'script_name = {_toml_string(settings.source.script_name)}',
        'api_key = { env = "SG_EVENT_TEST_SOURCE_KEY" }',
    ]
    if not production_requests:
        blocks.extend(
            (
                'max_rpc_attempts = 1',
                'rpc_attempt_interval_secs = 0.1',
                'timeout_secs = 5.0',
            )
        )
    blocks.extend(
        (
            '',
            '[sites.live.sources.events]',
            'credentials = "source"',
            'fetch_interval_secs = 0.05',
        )
    )
    if late_event_window_secs is not None:
        blocks.append(f'late_event_window_secs = {late_event_window_secs}')
    if groups is None:
        groups = tuple(LiveGroupSpec(name, name, 'eager', name) for name in group_names)
    for spec in groups:
        group_name = spec.name
        output_path = paths[group_name].resolve()
        plugin_path = fixture_directory / f'{spec.probe}_probe.py'
        if spec.probe == 'bulk' and project_id is None:
            raise ValueError('bulk probe requires project_id')
        blocks.extend(
            (
                '',
                f'[sites.live.groups.{group_name}]',
                'callback_credentials = "source"',
                f'execution = {_toml_string(spec.execution)}',
                f'ordering = {_toml_string(spec.ordering)}',
            )
        )
        if spec.worker == 'dedicated':
            blocks.append('worker = "dedicated"')
        blocks.extend(
            (
                '',
                f'[[sites.live.groups.{group_name}.plugins]]',
                f'id = {_toml_string(f"{group_name}-probe")}',
                f'path = {_toml_string(plugin_path)}',
                '',
                f'[sites.live.groups.{group_name}.plugins.config]',
                f'project_id = {project_id}' if spec.probe == 'bulk' else f'shot_id = {shot_id}',
                f'output_path = {_toml_string(output_path)}',
            )
        )
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text('\n'.join(blocks) + '\n', encoding='utf-8')
    return config_path


def write_report(
    report: LiveReport,
    report_dir: Path | None,
    *,
    failures: Iterable[BaseException] = (),
    terminal: TextIO | None = sys.stdout,
) -> Path | None:
    report.failure_categories = [
        failure.category for failure in failures if isinstance(failure, SanitizedLiveError)
    ]
    if terminal is not None:
        terminal.write(
            f'live acceptance mode={report.mode} expected={len(report.expected_ids)} '
            f'source-visible={report.source_visible_count} '
            f'successful-updates={report.successful_updates}/{report.requested_updates}\n'
        )
    if report_dir is None:
        return None
    directory = report_dir.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')
    path = directory / f'live-acceptance-{timestamp}.json'
    path.write_text(
        json.dumps(asdict(report), ensure_ascii=True, separators=(',', ':')) + '\n',
        encoding='utf-8',
    )
    return path
