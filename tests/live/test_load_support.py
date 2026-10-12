from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from tests.live._daemon import _LiveReport, _ProbeRecord, _ProducerProgress
from tests.live._load import (
    _analyze_group_deliveries,
    _GroupDeliveryMetrics,
    _LoadPhases,
    assert_load_deliveries,
    load_timeout,
    partition_load,
    perform_updates,
)
from tests.live._site import LiveSite

if TYPE_CHECKING:
    from tests._live_mode import _LiveMode, _LiveProvider


class _FailingUpdateSite(LiveSite):
    _fail_on_call: int
    _namespace: str
    _update_calls: int
    _values: list[str]

    def __init__(self) -> None:
        self._fail_on_call = 1_001
        self._namespace = 'acceptance-run'
        self._update_calls = 0
        self._values = []

    def _update_description(self, value: str) -> None:
        self._update_calls += 1
        if self._update_calls == self._fail_on_call:
            raise RuntimeError('update failed')
        self._values.append(value)


def _record(event_id: int, phase_value: str | None = None) -> _ProbeRecord:
    return _ProbeRecord(
        event_id=event_id,
        event_type='Shotgun_Shot_Change',
        attribute_name='description',
        entity_type='Shot',
        entity_id=41,
        delivery_attempt=1,
        is_operator_replay=False,
        group='sync',
        phase_value=phase_value,
    )


@pytest.fixture
def failing_site() -> _FailingUpdateSite:
    return _FailingUpdateSite()


@pytest.fixture
def report() -> _LiveReport:
    return _LiveReport(
        site_hostname='example.invalid',
        project_name='Live Acceptance',
        provider='sg-python-api',
        mode='sync+async',
        namespace='acceptance-run',
    )


def test_delivery_assertion_allows_inversion_but_rejects_loss_or_duplicate() -> None:
    assert_load_deliveries(
        dict.fromkeys((101, 102)).keys(),
        {'sync': (record for record in (_record(102), _record(101)))},
    )
    with pytest.raises(AssertionError, match='missing'):
        assert_load_deliveries((101, 102), {'sync': (_record(101),)})
    with pytest.raises(AssertionError, match='duplicate'):
        assert_load_deliveries((101,), {'sync': (_record(101), _record(101))})


def test_delivery_assertion_rejects_divergent_group_sets() -> None:
    with pytest.raises(AssertionError, match='different event sets'):
        assert_load_deliveries(
            (101, 102),
            {'async': (_record(102),), 'sync': (_record(101),)},
        )


def test_delivery_metrics_exclude_unrelated_records() -> None:
    metrics = _analyze_group_deliveries(
        expected_ids=(event_id for event_id in (101,)),
        records=(record for record in (_record(999), _record(101), _record(999))),
    )
    assert metrics == _GroupDeliveryMetrics(
        _delivered_unique_ids=(101,),
        _duplicate_ids=(),
        _inversions=(),
        _missing_ids=(),
    )


def test_delivery_metrics_report_missing_duplicates_and_inversions() -> None:
    metrics = _analyze_group_deliveries(
        expected_ids=(101, 102, 103),
        records=(_record(102), _record(101), _record(102)),
    )
    assert metrics._missing_ids == (103,)
    assert metrics._duplicate_ids == (102,)
    assert metrics._inversions == ((102, 101),)
    assert metrics._delivered_unique_ids == (101, 102)


@pytest.mark.parametrize('count', [1, 1_001])
def test_load_helpers_reject_counts_outside_finite_bound(count: int) -> None:
    with pytest.raises(ValueError, match='between 2 and 1000'):
        partition_load(count)
    with pytest.raises(ValueError, match='between 2 and 1000'):
        load_timeout(count)


def test_load_timeout_is_bounded_by_base_timeout_and_count() -> None:
    assert load_timeout(2) == 122.0
    assert load_timeout(1_000) == 1_120.0


@pytest.mark.parametrize(
    ('count', 'expected'),
    [(2, (1, 1)), (3, (2, 1)), (1_000, (500, 500))],
)
def test_partition_load_keeps_both_phases_nonempty(
    count: int,
    expected: tuple[int, int],
) -> None:
    phases = partition_load(count)
    assert (phases._live_count, phases._stopped_count) == expected


def test_perform_updates_uses_sequential_zero_padded_phase_values(
    failing_site: _FailingUpdateSite,
    report: _LiveReport,
) -> None:
    progress = perform_updates(failing_site, 7, 2, 'stopped', report)

    assert progress.phase == 'stopped'
    assert progress.requested_updates == 2
    assert progress.successful_updates == 2
    assert progress.phase_values == [
        'acceptance-run-load-000007',
        'acceptance-run-load-000008',
    ]
    assert failing_site._values == progress.phase_values
    assert report.expected_phases == progress.phase_values


def test_producer_progress_survives_update_failure(
    failing_site: _FailingUpdateSite,
    report: _LiveReport,
) -> None:
    failing_site._fail_on_call = 417

    with pytest.raises(RuntimeError, match='update failed'):
        perform_updates(failing_site, 0, 1_000, 'live', report)

    progress = report.producer_progress['live']
    assert progress.requested_updates == 1_000
    assert progress.successful_updates == 416
    assert len(progress.phase_values) == 416
    assert progress.elapsed_secs > 0
    assert report.requested_updates == 1_000
    assert report.successful_updates == 416


@pytest.mark.parametrize(
    ('provider', 'mode'),
    [('sg-python-api', 'sync+async'), ('shotgun-api3', 'sync')],
)
def test_record_load_derives_rate_without_a_pass_threshold(
    provider: _LiveProvider,
    mode: _LiveMode,
) -> None:
    report = _LiveReport(
        site_hostname='example.invalid',
        project_name='Live Acceptance',
        provider=provider,
        mode=mode,
        namespace='acceptance-run',
        successful_updates=3,
        producer_progress={
            'live': _ProducerProgress('live', 2, 2, elapsed_secs=4.0),
            'stopped': _ProducerProgress('stopped', 1, 1, elapsed_secs=2.0),
        },
    )
    metrics = {
        'sync': _GroupDeliveryMetrics((101, 102, 103), (), ((102, 101),), ()),
    }
    final_status = {
        'groups': ({'cursor': 103, 'group': 'sync', 'status': 'running'},),
        'liveness': True,
        'readiness': True,
        'summary': '<redacted>',
    }

    report._record_load(
        _LoadPhases(2, 1),
        metrics,
        final_status=final_status,
        live_convergence_secs=1.25,
        restart_convergence_secs=2.5,
    )

    assert report.achieved_rate == pytest.approx(0.5)
    assert report.timings == {
        'live_producer_secs': 4.0,
        'live_convergence_secs': 1.25,
        'producer_secs': 6.0,
        'restart_convergence_secs': 2.5,
        'stopped_producer_secs': 2.0,
    }
    assert report.status_summary == final_status
    assert report.load_metrics == {
        'phases': {'live_count': 2, 'stopped_count': 1},
        'sync': {
            'delivered_unique_count': 3,
            'delivered_unique_ids': (101, 102, 103),
            'duplicate_count': 0,
            'duplicate_ids': (),
            'inversion_count': 1,
            'inversions': ((102, 101),),
            'missing_count': 0,
            'missing_ids': (),
        },
    }
