import time
from collections import Counter
from collections.abc import Mapping
from typing import cast

import pytest

from tests.live._daemon import (
    LiveCaseFactory,
    assert_group_cursors,
    event_ids,
    read_all_groups,
    wait_for_group_event_set,
)
from tests.live._load import (
    BulkCriteriaEvidence,
    VisibilityObserver,
    assert_load_deliveries,
    evaluate_bulk_criteria,
    load_timeout,
    partition_load,
    perform_bulk_load,
    perform_updates,
    read_closeout_requests,
    read_http_attempts,
    read_request_timeline,
    read_worker_failures,
    record_bulk_catch_up_cpu,
    record_bulk_deliveries,
    record_bulk_visibility,
    sample_resident_memory,
)
from tests.live._site import live_event

pytestmark = pytest.mark.live_load


def binding_bulk_failures(acceptance: Mapping[str, object]) -> list[str]:
    descriptive = set(cast('list[str]', acceptance['descriptive_criteria']))
    return [
        f'{name}: {verdict}'
        for name, verdict in cast('Mapping[str, str]', acceptance['criteria']).items()
        if verdict != 'passed' and name not in descriptive
    ]


def test_live_bulk_load_delivers_late_events(live_case_factory: LiveCaseFactory) -> None:
    with live_case_factory(bulk=True) as live_case:
        site = live_case.site
        report = live_case.report
        observer = VisibilityObserver(site, after_id=live_case.baseline_event.id)
        live_case.process.start()
        assert live_case.process.pid is not None
        with sample_resident_memory(live_case.process.pid, report):
            live_case.process.wait_ready()
            observer.start()
            try:
                progress = perform_bulk_load(
                    site, duration_secs=60.0, report=report, daemon_pid=live_case.process.pid
                )
                # Keep observing for two windows after writes stop, including lost-response rows.
                time.sleep(60.0)
            finally:
                observer.finish()
                owned_ids = site.bulk_owned_ids()
                observer.select_events(owned_ids)
                expected = record_bulk_visibility(observer, owned_ids, report)
                site.observed_events = tuple(
                    live_event(observer.events[key]) for key in sorted(expected)
                )
            deadline = time.monotonic() + 120.0
            while True:
                records = read_all_groups(live_case)
                if all(
                    expected.issubset({row.event_id for row in rows}) for rows in records.values()
                ):
                    break
                if time.monotonic() >= deadline:
                    break
                time.sleep(0.1)
            record_bulk_catch_up_cpu(report, progress, live_case.process.pid)
            final_status = live_case.process.status()
            report.status_summary = dict(final_status)
            report.load_metrics['control_socket_buffers'] = {
                cast('str', worker['worker_id']): {
                    name: worker[name]
                    for name in (
                        'socket_buffer_requested',
                        'socket_send_buffer',
                        'socket_receive_buffer',
                    )
                }
                for worker in cast('list[Mapping[str, object]]', final_status['workers'])
            }
            assert live_case.process.stop() == ()
            failures = read_worker_failures(live_case.config_path.with_name('state'))
            report.load_metrics['worker_failures'] = dict(failures)
            records = read_all_groups(live_case)
            report.group_records = {
                group: [row for row in rows if row.event_id in expected]
                for group, rows in records.items()
            }
            report.load_metrics['closeout_requests'] = dict(
                read_closeout_requests(
                    live_case.config_path.with_name('requests.jsonl'),
                    live_case.settings.options.provider,
                    ['bulk-strict'],
                )
            )
            requests = read_request_timeline(
                live_case.config_path.with_name('requests.jsonl'),
                live_case.settings.options.provider,
            )
            report.load_metrics['requests'] = {
                'measured': live_case.settings.options.provider == 'shotgun-api3',
                'calls': requests,
            }
            attempts = read_http_attempts(
                live_case.config_path.with_name('requests.jsonl'),
                live_case.settings.options.provider,
            )
            report.load_metrics['http_attempts'] = {
                'calls': attempts,
                'counts': dict(
                    Counter(
                        str(row['status']) if row['status'] is not None else str(row['error'])
                        for row in attempts
                    )
                ),
            }
            late_ids = record_bulk_deliveries(
                observer, expected, records, report, requests=requests
            )
            acceptance = evaluate_bulk_criteria(
                report,
                BulkCriteriaEvidence(
                    progress=progress,
                    observer=observer,
                    expected_ids=expected,
                    records_by_group=records,
                    late_ids=late_ids,
                    failures=failures,
                    log_paths=live_case.process.log_paths,
                    duration_secs=60.0,
                    sanitize_text=live_case.process.sanitize_text,
                ),
            )
            report.load_metrics['acceptance'] = acceptance
            if acceptance['outcome'] != 'passed':
                failed = binding_bulk_failures(acceptance)
                if acceptance['site_slow']:
                    failed.append(
                        'SG site slow: longest observer completion interval '
                        f'{acceptance["observer_max_completion_interval_secs"]}s'
                    )
                pytest.fail('; '.join(failed))


def test_live_load_converges_across_restart(
    live_case_factory: LiveCaseFactory,
) -> None:
    with live_case_factory() as live_case:
        count = live_case.settings.options.load_events
        phases = partition_load(count)
        boundary = live_case.baseline_event.id

        live_case.process.start()
        live_case.process.wait_ready()
        live_progress = perform_updates(
            live_case.site,
            0,
            phases.live_count,
            'live',
            live_case.report,
        )
        live_events = live_case.site.wait_for_events(
            after_id=boundary,
            expected_values=tuple(live_progress.phase_values),
            timeout_secs=load_timeout(count),
        )
        live_convergence_started = time.monotonic()
        wait_for_group_event_set(live_case, live_events)
        live_convergence_secs = time.monotonic() - live_convergence_started
        live_case.report.timings['live_convergence_secs'] = live_convergence_secs

        assert live_case.process.stop() == ()
        stopped_progress = perform_updates(
            live_case.site,
            phases.live_count,
            phases.stopped_count,
            'stopped',
            live_case.report,
        )
        expected_values = tuple(live_progress.phase_values + stopped_progress.phase_values)
        all_events = live_case.site.wait_for_events(
            after_id=boundary,
            expected_values=expected_values,
            timeout_secs=load_timeout(count),
        )

        live_case.process.start()
        restart_convergence_started = time.monotonic()
        wait_for_group_event_set(live_case, all_events)
        restart_convergence_secs = time.monotonic() - restart_convergence_started
        live_case.report.timings['restart_convergence_secs'] = restart_convergence_secs
        final_status = live_case.process.wait_ready()
        live_case.report.status_summary = dict(final_status)
        expected_ids = event_ids(all_events)
        metrics = assert_load_deliveries(expected_ids, read_all_groups(live_case))
        assert_group_cursors(
            final_status,
            expected_groups=live_case.output_paths,
            expected_floor=max(expected_ids),
        )
        live_case.report.expected_phases = list(expected_values)
        live_case.report.expected_ids = list(expected_ids)
        live_case.report.source_visible_count = len(all_events)
        live_case.report.record_load(
            phases,
            metrics,
            final_status=final_status,
            live_convergence_secs=live_convergence_secs,
            restart_convergence_secs=restart_convergence_secs,
        )
