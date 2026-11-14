import pytest

from tests.live._daemon import (
    LIVE_TIMEOUT_SECS,
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
        process = live_case.process
        site = live_case.site
        baseline_id = live_case.baseline_event.id

        process.start()
        process.wait_ready()

        live_1_value = f'{site.namespace}-live-1'
        live_case.report.expected_phases.append(live_1_value)
        site.update_description(live_1_value)
        live_1 = site.wait_for_events(
            after_id=baseline_id,
            expected_values=(live_1_value,),
            timeout_secs=LIVE_TIMEOUT_SECS,
        )[0]
        wait_for_group_records(live_case, expected_events=(live_1,))

        assert process.stop() == ()
        stopped_value = f'{site.namespace}-stopped'
        live_case.report.expected_phases.append(stopped_value)
        site.update_description(stopped_value)
        stopped = site.wait_for_events(
            after_id=live_1.id,
            expected_values=(stopped_value,),
            timeout_secs=LIVE_TIMEOUT_SECS,
        )[0]

        process.start()
        wait_for_group_records(live_case, expected_events=(live_1, stopped))

        live_2_value = f'{site.namespace}-live-2'
        live_case.report.expected_phases.append(live_2_value)
        site.update_description(live_2_value)
        live_2 = site.wait_for_events(
            after_id=stopped.id,
            expected_values=(live_2_value,),
            timeout_secs=LIVE_TIMEOUT_SECS,
        )[0]
        expected = (live_1, stopped, live_2)
        wait_for_group_records(live_case, expected_events=expected)
        final_status = process.wait_ready()
        assert_group_deliveries(live_case, expected, excluded_ids=(baseline_id,))
        assert_group_cursors(
            final_status,
            expected_groups=live_case.output_paths,
            expected_floor=live_2.id,
        )
