import time

import pytest

from tests.live._daemon import (
    LiveCaseFactory,
    _event_ids,
    _read_all_groups,
    assert_group_cursors,
    wait_for_group_event_set,
)
from tests.live._load import (
    assert_load_deliveries,
    load_timeout,
    partition_load,
    perform_updates,
)

pytestmark = pytest.mark.live_load


def test_live_load_converges_across_restart(
    live_case_factory: LiveCaseFactory,
) -> None:
    with live_case_factory() as live_case:
        count = live_case._settings._options._load_events
        phases = partition_load(count)
        boundary = live_case._baseline_event._id

        live_case._process._start()
        live_case._process._wait_ready()
        live_progress = perform_updates(
            live_case._site,
            0,
            phases._live_count,
            'live',
            live_case._report,
        )
        live_events = live_case._site._wait_for_events(
            after_id=boundary,
            expected_values=tuple(live_progress.phase_values),
            timeout_secs=load_timeout(count),
        )
        live_convergence_started = time.monotonic()
        wait_for_group_event_set(live_case, live_events)
        live_convergence_secs = time.monotonic() - live_convergence_started
        live_case._report.timings['live_convergence_secs'] = live_convergence_secs

        assert live_case._process._stop() == ()
        stopped_progress = perform_updates(
            live_case._site,
            phases._live_count,
            phases._stopped_count,
            'stopped',
            live_case._report,
        )
        expected_values = tuple(live_progress.phase_values + stopped_progress.phase_values)
        all_events = live_case._site._wait_for_events(
            after_id=boundary,
            expected_values=expected_values,
            timeout_secs=load_timeout(count),
        )

        live_case._process._start()
        restart_convergence_started = time.monotonic()
        wait_for_group_event_set(live_case, all_events)
        restart_convergence_secs = time.monotonic() - restart_convergence_started
        live_case._report.timings['restart_convergence_secs'] = restart_convergence_secs
        final_status = live_case._process._wait_ready()
        live_case._report.status_summary = dict(final_status)
        expected_ids = _event_ids(all_events)
        metrics = assert_load_deliveries(expected_ids, _read_all_groups(live_case))
        assert_group_cursors(
            final_status,
            expected_groups=live_case._output_paths,
            expected_floor=max(expected_ids),
        )
        live_case._report.expected_phases = list(expected_values)
        live_case._report.expected_ids = list(expected_ids)
        live_case._report.source_visible_count = len(all_events)
        live_case._report._record_load(
            phases,
            metrics,
            final_status=final_status,
            live_convergence_secs=live_convergence_secs,
            restart_convergence_secs=restart_convergence_secs,
        )
