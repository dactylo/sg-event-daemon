"""Measure sustained offline delivery through the real supervisor and workers.

Run from the checkout root with `python -m benchmarks.delivery_speed --groups
alone|paired --work-dir NEW_DIRECTORY`. Every outcome retains raw evidence and
`report.json`; only a validated, quiet-host floor pass exits zero. The
prospective typical-capacity gate uses a fixed eight-run median assessment,
with one deadline and launch budget across host replacements and a conditional
larger-burst sequence.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import platform
import re
import signal
import sqlite3
import statistics
import subprocess
import sys
import time
from contextlib import closing
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict, TypeGuard, Unpack, cast

from sg_event_daemon._control import ControlClient, ControlUnavailableError
from sg_event_daemon._platform import spawn_child
from tests._host_sampling import HostSampler

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = ['analyze_run', 'assess_sequence', 'run_benchmark']

_ALONE_FLOOR = 1400
_ALONE_SLOTS = (1, 4, 7)
_BASE_ID = 1_000_000
_DATABASE_COUNT = 2
_FALLBACK_BURST = 12_000
_KILL_ATTEMPT = 20
_LOGGER = logging.getLogger(__name__)
_MAX_SEQUENCE_LAUNCHES = 16
_MAX_SLOT_LAUNCHES = 2
_MIN_BURST = 10_000
_MIN_INTERVAL = 3.0
_OUTSTANDING = 1_000
_PAIRED_FLOOR = 1000
_PREFLIGHT_INTERVAL = 30.0
_PREFLIGHT_PAIR = 2
_PROBE = Path(__file__).resolve().parents[1] / 'tests/live/fixtures/bulk_probe.py'
_QUIET_LIMIT = 60.0
_REPO = Path(__file__).resolve().parents[1]
_SAMPLE_INTERVAL = 1.0
_SEQUENCE_SECONDS = 1800.0
_SEQUENCE_SLOTS = 8
_STRESS_LIMIT = 80.0
_WARMUP = 4_000


class _MeasurementOptions(TypedDict):
    """Describe decoded measurement inputs independently of run orchestration."""

    paired: bool
    warmup: int
    records: dict[str, list[dict[str, Any]]]
    publications: list[dict[str, Any]]
    transactions: list[dict[str, Any]]
    wall_offset: float
    host_samples: list[dict[str, Any]]
    launch_monotonic: float
    finish_monotonic: float
    capacity_detaches: list[dict[str, object]]


class _ProbeCounter:
    """Track complete probe lines without rereading the growing files."""

    def __init__(self, directory: Path, groups: list[str]) -> None:
        """Initialize incremental offsets for every required group."""
        self._directory = directory
        self._offsets = dict.fromkeys(groups, 0)
        self._counts = dict.fromkeys(groups, 0)
        self._pending = dict.fromkeys(groups, '')

    def wait(self, target: int, process: subprocess.Popen[bytes]) -> None:
        """Wait for every group's target or preserve a child/timeout failure.

        Args:
            target: Total callback count, including warm-up.
            process: The owned supervisor; early exit aborts the wait.

        Raises:
            OSError: A probe record file cannot be read.
            RuntimeError: The supervisor exits before every group reaches the
                target.
            TimeoutError: Every group has not reached the target within
                240 seconds.
        """
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            for group, offset in self._offsets.items():
                path = self._directory / f'{group}.jsonl'
                if not path.exists():
                    continue
                with path.open() as stream:
                    stream.seek(offset)
                    data = self._pending[group] + stream.read()
                    self._offsets[group] = stream.tell()
                lines = data.split('\n')
                self._pending[group] = lines.pop()
                self._counts[group] += len(lines)
            if all(value >= target for value in self._counts.values()):
                return
            if process.poll() is not None:
                raise RuntimeError(f'daemon exited {process.returncode}')
            time.sleep(0.1)
        raise TimeoutError(f'delivery stopped at {self._counts}, expected {target}')


class _SequenceAssessment:
    """Validate the bounded launch schedule without reordering or dropping evidence."""

    def __init__(self, start: float, finish: float) -> None:
        """Initialize the one immutable observation budget."""
        self._start = start
        self._finish = finish
        self._deadline = start + _SEQUENCE_SECONDS
        self._rows: list[dict[str, Any]] = []
        self._errors: list[str] = []
        self._identity: dict[str, Any] | None = None
        self._paths: set[str] = set()
        self._current: list[dict[str, Any]] = []
        self._burst = _MIN_BURST
        if not _finite(start) or not _finite(finish) or finish < start:
            self._errors.append('invalid sequence clock bounds')

    def _fallback(self, row: dict[str, Any]) -> None:
        """Allow only the conditional complete larger sequence within the original budget."""
        slots = {item['slot'] for item in self._current}
        short = any(item['classification']['interval'] == 'short' for item in self._current)
        eligible = all(
            not item['invalid']
            and item['classification']['correctness'] == 'passed'
            and item['classification']['capacity'] == 'attached'
            and item['report']['validation']['all_group_publication'] is True
            and item['preflight_check']['eligible']
            and (
                item['host_check']['eligible']
                or (
                    item['classification']['interval'] == 'short'
                    and item['host_check']['lifetime_eligible']
                )
            )
            for item in self._current
        )
        if (
            self._burst != _MIN_BURST
            or row['burst'] != _FALLBACK_BURST
            or slots != set(range(1, _SEQUENCE_SLOTS + 1))
            or not short
            or not eligible
        ):
            raise ValueError('unauthorized size fallback or restart')
        self._current = []
        self._burst = row['burst']

    @staticmethod
    def _groups(selected: list[dict[str, Any]], *, complete: bool) -> dict[str, object]:
        """Keep individual group rates separate from the paired-minimum decision."""
        return {
            mode: {
                name: _statistics(
                    [
                        row['rates'][name]
                        for row in selected
                        if (row['slot'] in _ALONE_SLOTS) == (mode == 'alone')
                    ],
                    complete=complete,
                )
                for name in names
            }
            for mode, names in [
                ('alone', ['bulk-eager']),
                ('paired', ['bulk-eager', 'bulk-strict']),
            ]
        }

    def _outcome(
        self,
        *,
        complete: bool,
        correctness: bool,
        bounded: bool,
        alone: list[float],
        paired: list[float],
    ) -> str:
        """Keep malformed evidence and correctness failures ahead of measurement exclusions."""
        if self._errors:
            return 'invalid-evidence'
        if correctness:
            return 'correctness-failure'
        if not complete or not bounded:
            return 'measurement-limitation'
        if statistics.median(alone) < _ALONE_FLOOR or statistics.median(paired) < _PAIRED_FLOOR:
            return 'floor-miss'
        return 'passed'

    def _slot(self, row: dict[str, Any]) -> None:
        """Permit only the next fixed slot or an immediate host-only replacement."""
        slot, attempt = row['slot'], row['attempt']
        if type(slot) is not int or not 1 <= slot <= _SEQUENCE_SLOTS or type(attempt) is not int:
            raise ValueError('invalid slot or attempt')
        previous = self._current[-1] if self._current else None
        if previous is not None and slot == previous['slot']:
            axes = previous['classification']
            if (
                attempt != _MAX_SLOT_LAUNCHES
                or previous['attempt'] != 1
                or axes['host'] == 'quiet'
                or axes['correctness'] != 'passed'
                or axes['capacity'] != 'attached'
                or axes['interval'] != 'measured'
            ):
                raise ValueError('replacement is not an immediate host-only second launch')
        elif attempt != 1 or slot != (previous['slot'] + 1 if previous else 1):
            raise ValueError('missing, repeated or reordered slot')

    def _validate(self, row: dict[str, Any], previous_finish: float) -> None:
        """Check identities, clocks, report uniqueness, slots and fallback authority."""
        identity = _identity(row)
        if self._identity is not None and identity != self._identity:
            raise ValueError(
                'changed session/code/environment/configuration/instrumentation identity'
            )
        self._identity = identity
        if row['raw_report'] in self._paths:
            raise ValueError('reused report path')
        self._paths.add(row['raw_report'])
        if (
            not previous_finish
            <= row['started_monotonic']
            <= row['finished_monotonic']
            <= self._finish
        ):
            raise ValueError('overlapping or inconsistent command schedule')
        if (
            len(self._rows) == 1
            and row['preflight']['observations'][0]['started_monotonic'] != self._start
        ):
            raise ValueError('budget does not start at first observation')
        if row['burst'] != self._burst:
            self._fallback(row)
        self._slot(row)

    def add(self, attempt: Mapping[str, Any]) -> None:
        """Retain and assess one launch in the supplied actual order.

        Malformed evidence and sequence-rule violations are retained on the
        attempt and in the assessment's errors rather than discarded.

        Args:
            attempt: One launch record containing its raw report, preflight
                observations, slot and attempt numbers, burst size, identities,
                and monotonic start and finish times. Records are supplied in
                actual launch order.
        """
        previous_finish = (
            self._rows[-1].get('finished_monotonic', self._start) if self._rows else self._start
        )
        row = _classify_attempt(attempt, previous_finish)
        self._rows.append(row)
        try:
            self._validate(row, previous_finish)
        except (IndexError, KeyError, TypeError, ValueError, OverflowError) as error:
            row['invalid'].append(str(error))
        self._errors.extend(row['invalid'])
        self._current.append(row)

    def result(self) -> dict[str, object]:
        """Summarize only the final same-size set and retain all original attempts.

        Returns:
            Gate outcome, reasons, timing and launch counts, retained attempts,
            selected launches, and per-mode and per-group statistics. The
            qualifying aggregate is `None` unless the full sequence is valid,
            correct, and within its original budget. `attempts` shares the
            assessment's retained list; later additions are visible there.
        """
        selected = [
            row
            for row in self._current
            if row['measured']
            and not row['invalid']
            and row['started_monotonic'] < self._deadline
            and row['finished_monotonic'] <= self._deadline
        ]
        complete = len(selected) == _SEQUENCE_SLOTS and {row['slot'] for row in selected} == set(
            range(1, _SEQUENCE_SLOTS + 1)
        )
        alone = [row['rates']['bulk-eager'] for row in selected if row['slot'] in _ALONE_SLOTS]
        paired = [
            min(row['rates'].values()) for row in selected if row['slot'] not in _ALONE_SLOTS
        ]
        stats = {
            'alone': _statistics(alone, complete=complete),
            'paired': _statistics(paired, complete=complete),
        }
        correctness = any(row['correctness_failures'] for row in self._rows)
        bounded = (
            len(self._rows) <= _MAX_SEQUENCE_LAUNCHES
            and self._finish <= self._deadline
            and all(
                row.get('started_monotonic', self._deadline) < self._deadline for row in self._rows
            )
        )
        outcome = self._outcome(
            complete=complete, correctness=correctness, bounded=bounded, alone=alone, paired=paired
        )
        return {
            'delivery_gate': 'passed' if outcome == 'passed' else 'unfulfilled',
            'outcome': outcome,
            'reasons': list(
                dict.fromkeys([*self._errors, *([] if outcome == 'passed' else [outcome])])
            ),
            'start_monotonic': self._start,
            'finish_monotonic': self._finish,
            'deadline_monotonic': self._deadline,
            'elapsed_secs': self._finish - self._start,
            'launch_count': len(self._rows),
            'launch_limit': _MAX_SEQUENCE_LAUNCHES,
            'attempts': self._rows,
            'selected_burst': self._burst if complete else None,
            'selected': [
                {'slot': row['slot'], 'raw_report': row['raw_report'], 'burst': row['burst']}
                for row in selected
            ],
            'qualifying_aggregate': stats
            if complete and bounded and not self._errors and not correctness
            else None,
            **stats,
            'groups': self._groups(selected, complete=complete),
        }


def _capacity_detaches(directory: Path) -> list[dict[str, object]]:
    """Read every capacity detach from retained child logs across the whole run."""
    pattern = re.compile(
        r'^(\S+) INFO sg_event_daemon\.source: detached group (\S+) '
        r'from its live channel: ('
        r'queue-count-exhausted|queue-byte-exhausted|strict-buffer-capacity)'
        r'(?=\s|$)'
    )
    found: list[dict[str, object]] = []
    for path in sorted(directory.glob('child-*.log')):
        if not path.name.endswith(('.stdout.log', '.stderr.log')):
            continue
        with path.open() as stream:
            for line_number, line in enumerate(stream, 1):
                if match := pattern.match(line):
                    found.append(
                        {
                            'group': match[2],
                            'reason': match[3],
                            'source_file': path.name,
                            'source_line': line_number,
                            'timestamp': match[1],
                        }
                    )
    return found


def _child_environment(directory: Path) -> dict[str, str]:
    """Remove inherited provider credentials and select hooks only for children."""
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(('SG_', 'SHOTGUN_', 'BURST_', 'DELIVERY_BENCHMARK_'))
    }
    environment.update(
        {
            'DELIVERY_BENCHMARK_DIR': str(directory),
            'DELIVERY_BENCHMARK_PARENT': str(os.getpid()),
            'DELIVERY_BENCHMARK_KEY': 'synthetic-offline-placeholder',
            'PYTHONPATH': os.pathsep.join(
                (str(_REPO / 'benchmarks/_delivery_speed_hooks'), str(_REPO / 'src'))
            ),
        }
    )
    return environment


def _database_checks(directory: Path) -> list[dict[str, object]]:
    """Inspect integrity and transient-state cleanup only after process-group exit."""
    results: list[dict[str, object]] = []
    for path in sorted((directory / 'state').rglob('*.db')):
        wal = Path(str(path) + '-wal')
        wal_empty = not wal.exists() or wal.stat().st_size == 0
        with closing(
            sqlite3.connect(f'{path.as_uri()}?mode=ro&immutable=1', uri=True)
        ) as connection:
            integrity = connection.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
            tables = {
                row[0]
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            counts: dict[str, int] = {}
            for table, query in {
                'discovery_ranges': 'SELECT COUNT(*) FROM discovery_ranges',
                'range_groups': 'SELECT COUNT(*) FROM range_groups',
                'late_ids': 'SELECT COUNT(*) FROM late_ids',
                'source_late_ids': 'SELECT COUNT(*) FROM source_late_ids',
                'epochs': 'SELECT COUNT(*) FROM epochs',
                'callback_marks': 'SELECT COUNT(*) FROM callback_marks',
            }.items():
                if table in tables:
                    counts[table] = connection.execute(query).fetchone()[0]
        results.append(
            {
                'path': str(path.relative_to(directory)),
                'integrity': integrity,
                'empty_transient_state': not any(counts.values()),
                'counts': counts,
                'wal_empty': wal_empty,
            }
        )
    return results


def _finite(value: object) -> TypeGuard[float | int]:
    """Recognize finite JSON numbers without treating booleans as observations."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _groups(paired: bool) -> list[str]:
    """Select the dedicated groups for one measured run."""
    return ['bulk-eager', 'bulk-strict'] if paired else ['bulk-eager']


def _healthy(status: dict[str, Any], groups: list[str], final_id: int) -> bool:
    """Require complete final source/group health after the final durable callback."""
    sources = status.get('sources', [])
    rows = status.get('groups', [])
    workers = status.get('workers', [])
    return bool(
        status.get('readiness')
        and status.get('liveness')
        and not status.get('projection_truncated')
        and len(sources) == 1
        and len(rows) == len(groups)
        and len(workers) == len(groups) + 1
        and {row['group'] for row in rows} == set(groups)
        and all(row['status'] == 'running' and not row['conditions'] for row in [*sources, *rows])
        and all(
            row['cursor'] == final_id
            and row['join_phase'] == 'attached'
            and not row['parked']
            and row['current_event_id'] is None
            and row['outstanding_late_id_count'] == 0
            and row['outstanding_late_obligation_count'] == 0
            and row['pending_catchup_epoch_count'] == 0
            for row in rows
        )
        and all(
            not row.get('breaker_open', False)
            and row['monitor_alive']
            and not row['stuck_threads']
            and row['watchdog_failure'] is None
            for row in workers
        )
        and sources[0]['frontier'] == final_id
        and sources[0]['late_id_count'] == 0
        and sources[0]['open_range_count'] == 0
        and sources[0]['pending_obligation_count'] == 0
    )


def _host_eligibility(
    samples: list[dict[str, Any]],
    start: float,
    end: float,
    launch: float,
    finish: float,
) -> dict[str, object]:
    """Require valid quiet samples before launch, within measurement, and through teardown."""
    reasons: list[str] = []
    timestamps = [row.get('monotonic', math.nan) for row in samples]
    valid_times = all(_finite(stamp) for stamp in timestamps)
    if not valid_times or timestamps != sorted(timestamps):
        reasons.append('invalid host sample timestamps')
    if not valid_times or not any(row['monotonic'] <= launch for row in samples):
        reasons.append('missing pre-run sample')
    if not samples or not valid_times or not end <= samples[-1]['monotonic'] <= finish:
        reasons.append('invalid sampling lifetime')
    lifetime_valid = not reasons
    if not valid_times or not any(start <= row['monotonic'] < end for row in samples):
        reasons.append('missing interval sample')
    sampling_valid = not reasons
    load_reasons: list[str] = []
    for row in samples:
        busy = row.get('busy_percent')
        if row.get('error') is not None or not _finite(busy):
            load_reasons.append('invalid host sample')
        elif not 0 <= busy <= _QUIET_LIMIT:
            load_reasons.append('host exceeded 60 percent busy')
    reasons.extend(load_reasons)
    return {
        'eligible': not reasons,
        'lifetime_eligible': lifetime_valid and not load_reasons,
        'classification': (
            'quiet'
            if not reasons
            else 'stress'
            if sampling_valid
            and all(
                _finite(row.get('busy_percent'))
                and row.get('error') is None
                and 0 <= row['busy_percent'] <= _STRESS_LIMIT
                for row in samples
            )
            and any(row['busy_percent'] > _QUIET_LIMIT for row in samples)
            else 'ineligible'
        ),
        'reason': '; '.join(dict.fromkeys(reasons)),
        'samples': samples,
    }


def _identity(attempt: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize only declared mode, burst and per-run directory identity fields."""
    identity = dict(attempt['identity'])
    for key in ('session', 'code', 'environment', 'instrumentation'):
        if not isinstance(identity.get(key), str) or not identity[key]:
            raise ValueError(f'missing {key} identity')
    configuration = dict(identity['configuration'])
    if (
        configuration.pop('paired') is not (attempt['slot'] not in _ALONE_SLOTS)
        or configuration.pop('burst') != attempt['burst']
        or str(Path(configuration.pop('work_dir')) / 'report.json') != attempt['raw_report']
        or not configuration
    ):
        raise ValueError('configuration identity disagrees with attempt')
    identity['configuration'] = configuration
    return identity


def _measure(**options: Unpack[_MeasurementOptions]) -> dict[str, Any]:
    """Evaluate a common half-open interval from raw callback and transaction observations."""
    paired, warmup = options['paired'], options['warmup']
    records, publications = options['records'], options['publications']
    transactions, wall_offset = options['transactions'], options['wall_offset']
    host_samples = options['host_samples']
    launch_monotonic, finish_monotonic = options['launch_monotonic'], options['finish_monotonic']
    groups = _groups(paired)
    validation: dict[str, bool] = {}
    validation['group_records'] = set(records) == set(groups)
    validation['all_group_publication'] = {row['group'] for row in publications} == set(groups)
    for group in groups:
        rows = records.get(group, [])
        validation[f'{group}:ids_order_once'] = [row['event_id'] for row in rows] == list(
            range(_BASE_ID + 1, _BASE_ID + 1 + len(rows))
        )
        validation[f'{group}:attempt_one'] = all(row['delivery_attempt'] == 1 for row in rows)
        validation[f'{group}:no_replay'] = all(row['is_operator_replay'] is False for row in rows)
        validation[f'{group}:sufficient_backlog'] = len(rows) - warmup > _OUTSTANDING
    capacity_detaches = options['capacity_detaches']
    report: dict[str, Any] = {
        'status': 'failed',
        'validation': validation,
        'groups': {},
        'capacity_detaches': capacity_detaches,
        'recovery_run': bool(capacity_detaches),
        'measurement_exclusion': 'capacity detach recovery' if capacity_detaches else None,
    }
    if not all(validation.values()):
        return report
    start = max(float(row['end']) for row in publications) + wall_offset + 0.25
    end = min(float(records[group][-_OUTSTANDING]['delivered_at']) for group in groups)
    duration = end - start
    report['interval'] = {
        'start_wall': start,
        'end_wall': end,
        'duration_secs': duration,
        'wall_offset': wall_offset,
    }
    host = _host_eligibility(
        host_samples, start - wall_offset, end - wall_offset, launch_monotonic, finish_monotonic
    )
    report['host'] = host
    for group in groups:
        rows = records[group][warmup:]
        count = sum(start <= row['delivered_at'] < end for row in rows)
        outstanding = sum(row['delivered_at'] >= end for row in rows)
        writes = [
            row
            for row in transactions
            if row['thread'] == f'sg-event-daemon:lane:{group}'
            and row['committed']
            and start <= row['started_at'] + wall_offset
            and row['finished_at'] + wall_offset < end
        ]
        validation[f'{group}:timing_order'] = all(
            math.isfinite(row['started_at'])
            and row['started_at'] <= row['begun_at'] <= row['finished_at']
            for row in writes
        )
        acquisition = sum(row['begun_at'] - row['started_at'] for row in writes)
        hold = sum(row['finished_at'] - row['begun_at'] for row in writes)
        rate = count / duration if duration > 0 else 0.0
        floor = _PAIRED_FLOOR if paired else _ALONE_FLOOR
        report['groups'][group] = {
            'callbacks': count,
            'rate': rate,
            'floor': floor,
            'floor_met': rate >= floor,
            'outstanding_at_start': sum(row['delivered_at'] >= start for row in rows),
            'outstanding_at_end': outstanding,
            'transactions': len(writes),
            'acquire_ms_per_event': 1000 * acquisition / count if count else None,
            'hold_ms_per_event': 1000 * hold / count if count else None,
        }
        validation[f'{group}:published_backlog'] = outstanding >= _OUTSTANDING
    if not all(validation.values()) or capacity_detaches:
        report['status'] = 'failed'
    elif duration < _MIN_INTERVAL:
        report['status'] = 'too-short'
    elif not host['eligible']:
        report['status'] = 'ineligible-host'
    elif all(row['floor_met'] for row in report['groups'].values()):
        report['status'] = 'passed'
    return report


def _preflight_observation(
    row: dict[str, Any], origin: float, previous: float, launch: float
) -> bool:
    """Check actual capture context against the external scheduling grid."""
    started, finished = row['started_monotonic'], row['finished_monotonic']
    tick = row['grid_index']
    if not all(_finite(value) for value in (started, finished)):
        return False
    if not isinstance(tick, int) or isinstance(tick, bool) or tick < 0:
        return False
    if not previous <= started <= finished <= launch or row['kind'] not in (
        'scheduled',
        'stop',
        'auxiliary',
        'missed',
    ):
        return False
    if row['kind'] == 'scheduled':
        due = origin + tick * _PREFLIGHT_INTERVAL
        if not due <= started < due + _PREFLIGHT_INTERVAL:
            return False
    if row['kind'] == 'missed':
        return row['sample'] is None
    return row['sample']['monotonic'] == finished


def _quiet(sample: dict[str, Any]) -> bool:
    """Require a real quiet observation, including an explicit successful capture."""
    busy = sample.get('busy_percent')
    return sample.get('error') is None and _finite(busy) and 0 <= busy <= _QUIET_LIMIT


def _preflight_check(
    preflight: dict[str, Any], previous_finish: float, launch: float
) -> dict[str, object]:
    """Find the final consecutive scheduled quiet pair without borrowing stop samples."""
    origin = preflight['grid_origin']
    observations = preflight['observations']
    valid = (
        _finite(origin)
        and previous_finish <= origin <= launch
        and preflight['interval_secs'] == _PREFLIGHT_INTERVAL
        and isinstance(preflight['errors'], list)
    )
    pair: list[int] = []
    last_tick = -1
    previous = origin
    for index, row in enumerate(observations):
        if not _preflight_observation(row, origin, previous, launch):
            valid = False
        previous = row['finished_monotonic']
        if row['kind'] == 'missed' or not _quiet(row['sample']):
            pair = []
        elif row['kind'] == 'scheduled':
            tick = row['grid_index']
            if tick <= last_tick:
                valid = False
            if tick != last_tick + 1:
                pair = []
            pair = [*pair[-1:], index]
            last_tick = tick
    fresh = bool(pair) and launch < origin + (last_tick + 1) * _PREFLIGHT_INTERVAL
    eligible = valid and not preflight['errors'] and len(pair) == _PREFLIGHT_PAIR and fresh
    return {'eligible': bool(eligible), 'valid_context': bool(valid), 'scheduled_pair': pair}


def _read_json(path: Path) -> dict[str, Any]:
    """Read a raw JSON object at the evidence boundary."""
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise TypeError(f'expected an evidence object: {path.name}')
    return cast('dict[str, Any]', value)


def _read_records(path: Path) -> list[dict[str, Any]]:
    """Load unchanged bulk-probe records after owned processes have exited."""
    return [cast('dict[str, Any]', json.loads(line)) for line in path.read_text().splitlines()]


def _report_context(attempt: Mapping[str, Any], groups: list[str]) -> None:
    """Reject report/command clock disagreement and malformed measured values."""
    report, metadata = attempt['report'], attempt['report']['metadata']
    clocks = [
        attempt['started_monotonic'],
        metadata['launch_monotonic'],
        metadata['finish_monotonic'],
        attempt['finished_monotonic'],
    ]
    if not all(_finite(value) for value in clocks) or clocks != sorted(clocks):
        raise ValueError('report lifetime is not contained in command lifetime')
    if (
        metadata['paired'] is not (attempt['slot'] not in _ALONE_SLOTS)
        or metadata['burst'] != attempt['burst']
        or metadata['warmup'] != _WARMUP
        or report['status'] not in ('passed', 'failed', 'too-short', 'ineligible-host')
        or attempt['exit_code'] not in (0, 1, 2)
    ):
        raise ValueError('report mode, workload or status disagrees with attempt')
    if (report['status'] == 'passed') != (attempt['exit_code'] == 0):
        raise ValueError('command exit disagrees with per-run status')
    interval = report.get('interval')
    if not interval:
        return
    numbers = [
        interval[name] for name in ('start_wall', 'end_wall', 'duration_secs', 'wall_offset')
    ]
    if not all(_finite(value) for value in numbers):
        raise ValueError('non-finite interval')
    start, end, duration, offset = numbers
    if (
        not math.isclose(end - start, duration)
        or not clocks[1] <= min(start, end) - offset <= max(start, end) - offset <= clocks[2]
    ):
        raise ValueError('interval disagrees with run timing')
    if set(report['groups']) != set(groups):
        raise ValueError('missing measured group')
    if duration < _MIN_INTERVAL:
        return
    if any(
        not all(
            _finite(row[key]) and row[key] >= 0
            for key in (
                'rate',
                'callbacks',
                'outstanding_at_start',
                'outstanding_at_end',
                'transactions',
                'acquire_ms_per_event',
                'hold_ms_per_event',
            )
        )
        for row in report['groups'].values()
    ):
        raise ValueError('missing or non-finite measurement')


def _report_correctness(report: dict[str, Any], groups: list[str]) -> list[str]:
    """Preserve every correctness failure, distinguishing absent publication evidence."""
    validation = report['validation']
    required = {
        'group_records',
        'all_group_publication',
        'workload',
        'complete_delivery',
        'healthy_final_status',
        'clean_exit',
        'all_children_instrumented',
        'transaction_evidence',
        'database_integrity_cleanup',
    }
    required.update(
        f'{group}:{name}'
        for group in groups
        for name in ('ids_order_once', 'attempt_one', 'no_replay', 'sufficient_backlog')
    )
    if report.get('interval'):
        required.update(
            f'{group}:{name}' for group in groups for name in ('timing_order', 'published_backlog')
        )
    absent_publication = validation.get('all_group_publication') is False and not report.get(
        'interval'
    )
    excluded: set[str] = {'all_group_publication'} if absent_publication else set()
    short_interval = report.get('interval', {}).get('duration_secs', _MIN_INTERVAL) < _MIN_INTERVAL
    if (absent_publication and not report['groups']) or short_interval:
        coverage = report.get('transaction_record_groups', [])
        if set(coverage) == set(groups):
            excluded.add('transaction_evidence')
    failures = [name for name in required if name not in validation]
    failures.extend(
        name for name, value in validation.items() if value is not True and name not in excluded
    )
    metadata = report['metadata']
    if (
        report.get('analysis_error')
        or metadata.get('error')
        or metadata.get('cleanup_error')
        or metadata.get('exit_code') != 0
        or metadata.get('cleanup_clean') is not True
        or metadata.get('process_group_gone') is not True
    ):
        failures.append('analysis, runtime or cleanup failure')
    return failures


def _classify_attempt(attempt: Mapping[str, Any], previous_finish: float) -> dict[str, Any]:
    """Keep independent exclusion axes beside every unchanged input report."""
    row = dict(attempt)
    row.update({'measured': False, 'rates': None, 'invalid': [], 'correctness_failures': []})
    row['classification'] = {
        'host': 'ineligible',
        'capacity': 'unknown',
        'interval': 'missing',
        'correctness': 'failed',
    }
    try:
        groups = (
            ['bulk-eager', 'bulk-strict']
            if attempt['slot'] not in _ALONE_SLOTS
            else ['bulk-eager']
        )
        _report_context(attempt, groups)
        report, metadata = attempt['report'], attempt['report']['metadata']
        failures = _report_correctness(report, groups)
        row['correctness_failures'] = failures
        row['classification']['correctness'] = 'failed' if failures else 'passed'
        interval = report.get('interval', {})
        row['classification']['interval'] = (
            'measured'
            if interval.get('duration_secs', 0) >= _MIN_INTERVAL
            else 'short'
            if interval
            else 'missing'
        )
        row['classification']['capacity'] = (
            'detached' if report['capacity_detaches'] else 'attached'
        )
        row['preflight_check'] = _preflight_check(
            attempt['preflight'], previous_finish, attempt['started_monotonic']
        )
        start = interval.get('start_wall', 0) - interval.get('wall_offset', 0)
        end = interval.get('end_wall', 0) - interval.get('wall_offset', 0)
        host = _host_eligibility(
            metadata['host_samples'],
            start,
            end,
            metadata['launch_monotonic'],
            metadata['finish_monotonic'],
        )
        row['host_check'] = host
        row['classification']['host'] = str(host['classification'])
        if not row['preflight_check']['eligible']:
            row['classification']['host'] = 'ineligible'
        if not row['preflight_check']['valid_context']:
            row['invalid'].append('invalid preflight scheduling context')
        row['measured'] = (
            not failures
            and row['classification']['host'] == 'quiet'
            and row['classification']['capacity'] == 'attached'
            and row['classification']['interval'] == 'measured'
        )
        if row['measured']:
            row['rates'] = {name: report['groups'][name]['rate'] for name in groups}
    except (IndexError, KeyError, TypeError, ValueError, OverflowError) as error:
        row['invalid'].append(f'malformed evidence: {error}')
    return row


def _statistics(rates: list[float], *, complete: bool) -> dict[str, object]:
    """Label descriptive partial statistics without inventing absent measurements."""
    return {
        'rates': rates,
        'median': statistics.median(rates) if rates else None,
        'minimum': min(rates) if rates else None,
        'maximum': max(rates) if rates else None,
        'partial': not complete,
    }


def _stop_unacknowledged(process: subprocess.Popen[bytes]) -> None:
    """Clean up only the known child when it never acknowledged a private session."""
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def _stop_owned(process: subprocess.Popen[bytes], directory: Path) -> dict[str, bool]:
    """Stop only this supervisor's session and confirm every owned process exited."""
    marker = directory / f'session-{process.pid}.json'
    owns_session = marker.exists() and _read_json(marker) == {
        'pid': process.pid,
        'sid': process.pid,
    }
    if not owns_session:
        _stop_unacknowledged(process)
        return {'process_group_gone': False, 'cleanup_clean': False}
    forced = False
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            forced = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
    for attempt in range(30):
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            return {'process_group_gone': True, 'cleanup_clean': not forced}
        if attempt == 0:
            forced = True
            os.killpg(process.pid, signal.SIGTERM)
        if attempt == _KILL_ATTEMPT:
            os.killpg(process.pid, signal.SIGKILL)
        time.sleep(0.1)
    return {'process_group_gone': False, 'cleanup_clean': False}


def _write_config(directory: Path, *, paired: bool) -> Path:
    """Keep diagnosis timing settings, one source, and dedicated probe groups."""
    text = f"""[daemon]
state_dir = {json.dumps(str(directory / 'state'))}
sg_api_provider = "shotgun-api3"
supervisor_poll_interval_secs = 0.05

[sites.live]
server = "https://offline.invalid"
default_source = "events"
[sites.live.credentials.source]
script_name = "offline-synthetic"
api_key = {{ env = "DELIVERY_BENCHMARK_KEY" }}
[sites.live.sources.events]
credentials = "source"
fetch_interval_secs = 0.05
late_event_window_secs = 30.0
"""
    for group in (name.removeprefix('bulk-') for name in _groups(paired)):
        text += f'''
[sites.live.groups.bulk-{group}]
callback_credentials = "source"
execution = "sync"
ordering = "{group}"
worker = "dedicated"
[[sites.live.groups.bulk-{group}.plugins]]
id = "bulk-{group}-probe"
path = {json.dumps(str(_PROBE))}
[sites.live.groups.bulk-{group}.plugins.config]
project_id = 1
output_path = {json.dumps(str(directory / f'bulk-{group}.jsonl'))}
'''
    path = directory / 'daemon.toml'
    path.write_text(text)
    return path


def _write_json(path: Path, value: object) -> None:
    """Retain readable evidence without converting nonfinite observations to null."""
    path.write_text(json.dumps(value, indent=2) + '\n')


def _release(directory: Path, count: int) -> None:
    """Atomically expose the whole warm-up or burst to every fake handle."""
    pending = directory / 'available.next'
    _write_json(pending, {'count': count})
    pending.replace(directory / 'available.json')


def _status(directory: Path, index: int) -> dict[str, Any]:
    """Retain each normal local control request, including startup failures."""
    try:
        result = dict(ControlClient.for_state_dir(directory / 'state').request('status'))
    except ControlUnavailableError as error:
        _write_json(directory / f'status-{index}.json', {'error': str(error)})
        return {}
    _write_json(directory / f'status-{index}.json', result)
    return result


def _wait_ready(directory: Path, process: subprocess.Popen[bytes], groups: list[str]) -> int:
    """Require all dedicated groups attached before exposing warm-up events."""
    deadline = time.monotonic() + 30
    index = 0
    while time.monotonic() < deadline:
        status = _status(directory, index)
        index += 1
        if (
            status.get('readiness')
            and len(status.get('groups', [])) == len(groups)
            and all(row.get('join_phase') == 'attached' for row in status['groups'])
        ):
            _write_json(directory / 'status-ready.json', status)
            return index
        if process.poll() is not None:
            raise RuntimeError(f'startup exited {process.returncode}')
        time.sleep(0.5)
    raise TimeoutError('workers did not attach')


def _deliver(
    directory: Path, process: subprocess.Popen[bytes], groups: list[str], metadata: dict[str, Any]
) -> None:
    """Release the fixed warm-up and whole burst, then await durable final health."""
    index = _wait_ready(directory, process, groups)
    warmup, burst = metadata['warmup'], metadata['burst']
    counter = _ProbeCounter(directory, groups)
    _release(directory, warmup)
    counter.wait(warmup, process)
    time.sleep(1.0)
    metadata['burst_release_monotonic'] = time.monotonic()
    metadata['burst_release_wall'] = time.time()
    _release(directory, warmup + burst)
    counter.wait(warmup + burst, process)
    metadata['all_delivered_monotonic'] = time.monotonic()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        status = _status(directory, index)
        index += 1
        _write_json(directory / 'status-final.json', status)
        if _healthy(status, groups, _BASE_ID + warmup + burst):
            return
        time.sleep(0.25)
    raise TimeoutError('final health did not settle')


def analyze_run(directory: Path) -> dict[str, object]:
    """Analyze retained evidence and write a reproducible `report.json`.

    Args:
        directory: A completed benchmark's evidence directory.

    Returns:
        Available measurement, validation, and host evidence with one of
        `passed`, `failed`, `too-short`, or `ineligible-host` as its `status`.
        Evidence-reading and analysis failures produce a partial failed report
        with `analysis_error`.

    Raises:
        OSError: The report cannot be written.
    """
    report: dict[str, Any] = {
        'status': 'failed',
        'validation': {},
        'capacity_detaches': [],
        'recovery_run': False,
        'measurement_exclusion': None,
    }
    try:
        report['capacity_detaches'] = _capacity_detaches(directory)
        report['recovery_run'] = bool(report['capacity_detaches'])
        if report['recovery_run']:
            report['measurement_exclusion'] = 'capacity detach recovery'
        metadata = _read_json(directory / 'run.json')
        report['metadata'] = metadata
        groups = _groups(metadata['paired'])
        records = {group: _read_records(directory / f'{group}.jsonl') for group in groups}
        timing = [_read_json(path) for path in sorted(directory.glob('timings-*.json'))]
        report.update(
            _measure(
                paired=metadata['paired'],
                warmup=metadata['warmup'],
                records=records,
                publications=[
                    row
                    for item in timing
                    for row in item['publications']
                    if row['event_id'] == _BASE_ID + metadata['warmup'] + metadata['burst']
                ],
                transactions=[row for item in timing for row in item['transactions']],
                wall_offset=metadata['burst_release_wall'] - metadata['burst_release_monotonic'],
                host_samples=metadata['host_samples'],
                launch_monotonic=metadata['launch_monotonic'],
                finish_monotonic=metadata['finish_monotonic'],
                capacity_detaches=report['capacity_detaches'],
            )
        )
        report['transaction_record_groups'] = [
            group
            for group in groups
            if any(
                row['thread'] == f'sg-event-daemon:lane:{group}'
                and row['committed']
                and all(_finite(row[key]) for key in ('started_at', 'begun_at', 'finished_at'))
                and row['started_at'] <= row['begun_at'] <= row['finished_at']
                for item in timing
                for row in item['transactions']
            )
        ]
        validation = report['validation']
        validation['workload'] = metadata['warmup'] == _WARMUP and metadata['burst'] >= _MIN_BURST
        validation['complete_delivery'] = all(
            len(rows) == metadata['warmup'] + metadata['burst'] for rows in records.values()
        )
        validation['healthy_final_status'] = _healthy(
            _read_json(directory / 'status-final.json'),
            groups,
            _BASE_ID + metadata['warmup'] + metadata['burst'],
        )
        validation['clean_exit'] = (
            metadata['exit_code'] == 0
            and metadata['process_group_gone']
            and metadata['cleanup_clean']
            and not metadata.get('error')
        )
        markers = [_read_json(path) for path in directory.glob('instrumented-*.json')]
        daemon_markers = [
            row
            for row in markers
            if any(command in row['argv'] for command in ('supervise', 'run-worker'))
        ]
        validation['all_children_instrumented'] = (
            len(daemon_markers) == len(groups) + 2
            and all(row['network_guard'] for row in markers)
            and {row['pid'] for row in markers} == {row['pid'] for row in timing}
        )
        validation['transaction_evidence'] = set(report['groups']) == set(groups) and all(
            row['transactions'] > 0 for row in report['groups'].values()
        )
        checks = _database_checks(directory)
        report['database_checks'] = checks
        validation['database_integrity_cleanup'] = len(checks) == _DATABASE_COUNT and all(
            row['integrity'] and row['empty_transient_state'] and row['wal_empty']
            for row in checks
        )
        if not all(validation.values()):
            report['status'] = 'failed'
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
        report['status'] = 'failed'
        report['analysis_error'] = f'{type(error).__name__}: {error}'
    _write_json(directory / 'report.json', report)
    return report


def assess_sequence(
    *, attempts: Sequence[Mapping[str, Any]], start_monotonic: float, finish_monotonic: float
) -> dict[str, object]:
    """Assess prospective typical capacity without launching work or rewriting reports.

    Args:
        attempts: Actual launch order, retaining each decoded report, raw path,
            slot/attempt/burst, command clocks and exit, identity, and
            preflight.
            Preflight has a 30-second grid origin, errors and ordered
            observations;
            each has kind, grid index, capture start/finish and the original
            sample.
            Only scheduled observations supply the consecutive quiet pair.
        start_monotonic: First host observation's capture start, including an
            invalid one.
        finish_monotonic: Schedule and cleanup completion, excluding later
            report writing.

    Returns:
        Independent attempt classifications and retained evidence, selected
        paths, explicitly partial or complete statistics, and the bounded
        median verdict.
    """
    assessment = _SequenceAssessment(start_monotonic, finish_monotonic)
    for attempt in attempts:
        assessment.add(attempt)
    return assessment.result()


def run_benchmark(
    *,
    work_dir: Path,
    paired: bool,
    burst: int = _MIN_BURST,
) -> dict[str, object]:
    """Run the fixed offline workload once and retain its outcome.

    Args:
        work_dir: New evidence directory; an existing path is never
            overwritten.
        paired: Include a strict group beside eager when true.
        burst: Events released together after warm-up; values below the minimum
            named in the error are rejected.

    Returns:
        The same report written to `work_dir/report.json`, including retained
        runtime or cleanup failures.

    Raises:
        ValueError: Workload dimensions violate the measurement contract.
        FileExistsError: The requested evidence directory already exists.
        OSError: Evidence directories or result files cannot be created or
            written.
    """
    if burst < _MIN_BURST:
        raise ValueError(f'burst must be at least {_MIN_BURST}')
    directory = work_dir.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    metadata: dict[str, Any] = {
        'paired': paired,
        'warmup': _WARMUP,
        'burst': burst,
        'page_delay_secs': 0.0,
        'host': {
            'platform': platform.platform(),
            'machine': platform.machine(),
            'logical_cpus': os.cpu_count(),
        },
        'sampler_interval_secs': _SAMPLE_INTERVAL,
        'start_monotonic': time.monotonic(),
    }
    _write_json(directory / 'run.json', metadata)
    sampler = HostSampler(os.getpid(), interval_secs=_SAMPLE_INTERVAL)
    process: subprocess.Popen[bytes] | None = None
    try:
        config = _write_config(directory, paired=paired)
        environment = _child_environment(directory)
        groups = _groups(paired)
        _release(directory, 0)
        sampler.start()
        metadata['launch_monotonic'] = time.monotonic()
        process = spawn_child(
            [sys.executable, '-m', 'sg_event_daemon', 'supervise', '--config', str(config)],
            env=environment,
        )
        metadata['supervisor_pid'] = process.pid
        _deliver(directory, process, groups, metadata)
    except BaseException as error:
        _LOGGER.exception('benchmark run failed; retaining evidence')
        metadata['error'] = f'{type(error).__name__}: {error}'
    finally:
        try:
            metadata.update(
                {'process_group_gone': True, 'cleanup_clean': False}
                if process is None
                else _stop_owned(process, directory)
            )
        except (OSError, subprocess.SubprocessError) as error:
            metadata['cleanup_error'] = f'{type(error).__name__}: {error}'
            metadata['process_group_gone'] = False
        metadata['exit_code'] = None if process is None else process.returncode
        try:
            sampler.stop()
        except BaseException as error:
            _LOGGER.exception('benchmark sampler teardown failed')
            metadata['sampling_error'] = f'{type(error).__name__}: {error}'
        metadata['host_samples'] = [asdict(sample) for sample in sampler.samples()]
        if metadata.get('sampling_error'):
            metadata['host_samples'].append(
                {
                    'monotonic': time.monotonic(),
                    'busy_percent': None,
                    'error': metadata['sampling_error'],
                }
            )
        metadata['finish_monotonic'] = time.monotonic()
        _write_json(directory / 'run.json', metadata)
    return analyze_run(directory)


def _main() -> int:
    """Return nonzero for every outcome except a fully qualified floor pass."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--groups', choices=('alone', 'paired'), required=True)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument(
        '--burst', type=int, default=_MIN_BURST, help=f'event count, at least {_MIN_BURST}'
    )
    args = parser.parse_args()
    report = run_benchmark(
        work_dir=args.work_dir,
        paired=args.groups == 'paired',
        burst=args.burst,
    )
    print(json.dumps(report, indent=2))
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(_main())
