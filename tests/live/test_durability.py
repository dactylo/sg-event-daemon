import pytest

from tests.live._daemon import (
    _LIVE_TIMEOUT_SECS,
    LiveCaseFactory,
    assert_group_cursors,
    assert_group_deliveries,
    wait_for_group_records,
)

pytestmark = pytest.mark.live


def test_live_delivery_survives_graceful_restart(
    live_case_factory: LiveCaseFactory,
) -> None:
    with live_case_factory() as live_case:
        process = live_case._process
        site = live_case._site
        baseline_id = live_case._baseline_event._id

        process._start()
        process._wait_ready()

        live_1_value = f'{site._namespace}-live-1'
        live_case._report.expected_phases.append(live_1_value)
        site._update_description(live_1_value)
        live_1 = site._wait_for_events(
            after_id=baseline_id,
            expected_values=(live_1_value,),
            timeout_secs=_LIVE_TIMEOUT_SECS,
        )[0]
        wait_for_group_records(live_case, expected_events=(live_1,))

        assert process._stop() == ()
        stopped_value = f'{site._namespace}-stopped'
        live_case._report.expected_phases.append(stopped_value)
        site._update_description(stopped_value)
        stopped = site._wait_for_events(
            after_id=live_1._id,
            expected_values=(stopped_value,),
            timeout_secs=_LIVE_TIMEOUT_SECS,
        )[0]

        process._start()
        wait_for_group_records(live_case, expected_events=(live_1, stopped))

        live_2_value = f'{site._namespace}-live-2'
        live_case._report.expected_phases.append(live_2_value)
        site._update_description(live_2_value)
        live_2 = site._wait_for_events(
            after_id=stopped._id,
            expected_values=(live_2_value,),
            timeout_secs=_LIVE_TIMEOUT_SECS,
        )[0]
        expected = (live_1, stopped, live_2)
        wait_for_group_records(live_case, expected_events=expected)
        final_status = process._wait_ready()
        assert_group_deliveries(live_case, expected, excluded_ids=(baseline_id,))
        assert_group_cursors(
            final_status,
            expected_groups=live_case._output_paths,
            expected_floor=live_2._id,
        )
