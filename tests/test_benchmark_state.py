from __future__ import annotations

import json
import socket
from typing import TYPE_CHECKING

import pytest

from benchmarks import benchmark_state

if TYPE_CHECKING:
    from pathlib import Path


class _SocketDouble:
    def __init__(self, option_values: dict[int, int]) -> None:
        self._option_values = option_values
        self._get_options: list[tuple[int, int]] = []
        self._set_options: list[tuple[int, int, int]] = []

    def close(self) -> None:
        pass

    def getsockopt(self, level: int, option: int) -> int:
        self._get_options.append((level, option))
        return self._option_values[option]

    def setsockopt(self, level: int, option: int, value: int) -> None:
        self._set_options.append((level, option, value))


def test_benchmark_batches_irrelevant_progress_and_reports_nonpositive_margin_first(
    tmp_path: Path,
) -> None:
    irrelevant = benchmark_state._run_benchmark(
        benchmark_state._BenchmarkConfig(
            rate=25.0,
            duration_secs=2.0,
            groups=13,
            match_ratio=0.0,
            callbacks_per_match=1,
            payload_bytes=256,
            numeric_gap_rate=0.0,
            ordering='eager',
            holdback_secs=1.0,
            sqlite_synchronous='normal',
            simulated_callback_latency_ms=0.0,
            simulated_storage_latency_ms=0.0,
            state_dir=tmp_path,
        )
    )
    assert irrelevant['matching_callbacks'] == 0
    assert irrelevant['sqlite_transactions'] < irrelevant['event_count']
    assert irrelevant['failures'] == []

    slow = benchmark_state._run_benchmark(
        benchmark_state._BenchmarkConfig(
            rate=25.0,
            duration_secs=1.0,
            groups=1,
            match_ratio=1.0,
            callbacks_per_match=1,
            payload_bytes=256,
            numeric_gap_rate=0.0,
            ordering='strict',
            holdback_secs=1.0,
            sqlite_synchronous='normal',
            simulated_callback_latency_ms=100.0,
            simulated_storage_latency_ms=0.0,
            state_dir=tmp_path,
        )
    )
    slow_strict = slow['strict_rejoin']
    assert slow_strict['delivery_rate_margin'] <= 0.0
    assert slow_strict['derived_rejoin_bound_secs'] is None
    assert 'strict catch-up delivery-rate margin is not positive' in slow['failures']


def test_benchmark_cli_accepts_state_dir_and_emits_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = benchmark_state._main(
        [
            '--rate',
            '25',
            '--groups',
            '13',
            '--duration-secs',
            '1',
            '--state-dir',
            str(tmp_path),
            '--json',
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload['event_count'] == 25
    assert payload['state_dir'] == str(tmp_path)
    assert 'strict_rejoin' in payload


def test_benchmark_configures_sender_and_receiver_buffers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sender = _SocketDouble({socket.SO_SNDBUF: 101, socket.SO_RCVBUF: 102})
    receiver = _SocketDouble({socket.SO_SNDBUF: 201, socket.SO_RCVBUF: 202})
    monkeypatch.setattr(
        benchmark_state.socket,
        'socketpair',
        lambda: (sender, receiver),
    )
    summary = benchmark_state._socket_buffers()

    assert sender._set_options == [(socket.SOL_SOCKET, socket.SO_SNDBUF, 8_388_608)]
    assert receiver._set_options == [(socket.SOL_SOCKET, socket.SO_RCVBUF, 8_388_608)]
    assert sender._get_options == [
        (socket.SOL_SOCKET, socket.SO_SNDBUF),
        (socket.SOL_SOCKET, socket.SO_RCVBUF),
    ]
    assert receiver._get_options == [
        (socket.SOL_SOCKET, socket.SO_SNDBUF),
        (socket.SOL_SOCKET, socket.SO_RCVBUF),
    ]
    assert summary == {
        'requested_send_bytes': 8_388_608,
        'sender_send_bytes': 101,
        'sender_receive_bytes': 102,
        'receiver_send_bytes': 201,
        'receiver_receive_bytes': 202,
        'minimum_asserted': False,
    }


def test_benchmark_detects_one_partially_dropped_event(tmp_path: Path) -> None:
    summary = benchmark_state._run_benchmark_scenario(
        benchmark_state._BenchmarkConfig(
            rate=10.0,
            duration_secs=1.0,
            groups=2,
            match_ratio=1.0,
            callbacks_per_match=1,
            payload_bytes=128,
            numeric_gap_rate=0.0,
            ordering='eager',
            holdback_secs=1.0,
            sqlite_synchronous='normal',
            simulated_callback_latency_ms=0.0,
            simulated_storage_latency_ms=0.0,
            state_dir=tmp_path,
        ),
        benchmark_state._SimulationFaults(_drop_event_ordinal=7),
    )

    assert summary['offered_event_groups'] == 20
    assert summary['accepted_event_groups'] == 19
    assert summary['delivered_event_groups'] == 19
    assert summary['dropped_events'] == 1
    assert summary['failures'][0] == 'workload dropped 1 event delivery'


def test_benchmark_detects_positive_margin_strict_nonconvergence(tmp_path: Path) -> None:
    summary = benchmark_state._run_benchmark_scenario(
        benchmark_state._BenchmarkConfig(
            rate=25.0,
            duration_secs=1.0,
            groups=1,
            match_ratio=1.0,
            callbacks_per_match=1,
            payload_bytes=128,
            numeric_gap_rate=0.0,
            ordering='strict',
            holdback_secs=1.0,
            sqlite_synchronous='normal',
            simulated_callback_latency_ms=10.0,
            simulated_storage_latency_ms=0.0,
            state_dir=tmp_path,
        ),
        benchmark_state._SimulationFaults(_block_strict_rejoin=True),
    )

    strict = summary['strict_rejoin']
    assert strict['delivery_rate_margin'] > 0.0
    assert strict['derived_rejoin_bound_secs'] is not None
    assert not strict['converged']
    assert strict['lag_events'] > 0
    assert strict['states'][-1] == 'fixed-boundary-rejoin'
    assert 'strict group did not converge within its measured bound' in summary['failures']


def test_benchmark_detects_queue_overflow_from_observed_queue_state(tmp_path: Path) -> None:
    summary = benchmark_state._run_benchmark_scenario(
        benchmark_state._BenchmarkConfig(
            rate=100.0,
            duration_secs=0.1,
            groups=1,
            match_ratio=1.0,
            callbacks_per_match=1,
            payload_bytes=128,
            numeric_gap_rate=0.0,
            ordering='eager',
            holdback_secs=1.0,
            sqlite_synchronous='normal',
            simulated_callback_latency_ms=50.0,
            simulated_storage_latency_ms=0.0,
            state_dir=tmp_path,
        ),
        benchmark_state._SimulationFaults(_queue_max_events=1),
    )

    assert summary['queue_peak']['events'] > summary['queue_bounds']['events']
    assert summary['queue_overflow_events'] > 0
    assert 'workload exceeded configured queue bounds' in summary['failures']


def test_benchmark_detects_residual_group_lag_after_drain(tmp_path: Path) -> None:
    summary = benchmark_state._run_benchmark_scenario(
        benchmark_state._BenchmarkConfig(
            rate=5.0,
            duration_secs=1.0,
            groups=2,
            match_ratio=1.0,
            callbacks_per_match=1,
            payload_bytes=128,
            numeric_gap_rate=0.0,
            ordering='eager',
            holdback_secs=1.0,
            sqlite_synchronous='normal',
            simulated_callback_latency_ms=0.0,
            simulated_storage_latency_ms=0.0,
            state_dir=tmp_path,
        ),
        benchmark_state._SimulationFaults(_stall_group=0),
    )

    assert summary['group_lag']['by_group'][0] == 5
    assert summary['group_lag']['maximum'] == 5
    assert summary['queued_event_groups'] == 5
    assert 'workload left group lag after drain' in summary['failures']


def test_benchmark_detects_strict_buffer_overflow_from_observed_state(
    tmp_path: Path,
) -> None:
    summary = benchmark_state._run_benchmark_scenario(
        benchmark_state._BenchmarkConfig(
            rate=10.0,
            duration_secs=0.3,
            groups=1,
            match_ratio=0.0,
            callbacks_per_match=1,
            payload_bytes=128,
            numeric_gap_rate=0.0,
            ordering='strict',
            holdback_secs=1.0,
            sqlite_synchronous='normal',
            simulated_callback_latency_ms=0.0,
            simulated_storage_latency_ms=0.0,
            state_dir=tmp_path,
        ),
        benchmark_state._SimulationFaults(_strict_max_events=1),
    )

    assert summary['strict_buffer_peak']['events'] > summary['strict_buffer_bounds']['events']
    assert summary['strict_buffer_overflow_events'] > 0
    assert 'workload exceeded configured strict-buffer bounds' in summary['failures']


def test_benchmark_reports_complete_deterministic_workload_and_strict_rejoin(
    tmp_path: Path,
) -> None:
    summary = benchmark_state._run_benchmark(
        benchmark_state._BenchmarkConfig(
            rate=25.0,
            duration_secs=2.0,
            groups=3,
            match_ratio=0.1,
            callbacks_per_match=2,
            payload_bytes=512,
            numeric_gap_rate=0.05,
            ordering='strict',
            holdback_secs=1.0,
            sqlite_synchronous='normal',
            simulated_callback_latency_ms=0.0,
            simulated_storage_latency_ms=0.0,
            state_dir=tmp_path,
        )
    )

    assert summary['event_count'] == 50
    assert summary['offered_rate'] == 25.0
    assert summary['group_count'] == 3
    assert summary['state_dir'] == str(tmp_path)
    assert summary['sqlite_mode'] == 'WAL/NORMAL'
    assert summary['dropped_events'] == 0
    assert summary['group_lag']['maximum'] == 0
    assert summary['sqlite_transactions'] < 50 * 3
    assert summary['checkpoints'] >= 1
    assert summary['payload_bytes'] >= 50 * 512
    assert summary['matching_callbacks'] > 0
    assert summary['commit_latency_ms']['p50'] >= 0.0
    assert summary['commit_latency_ms']['p95'] >= summary['commit_latency_ms']['p50']
    assert summary['commit_latency_ms']['p99'] >= summary['commit_latency_ms']['p95']
    assert summary['encoder_latency_ms']['p99'] >= summary['encoder_latency_ms']['p50']
    assert summary['decoded_object_memory_amplification'] > 1.0
    assert summary['commit_chunk_bytes']['count'] == summary['sqlite_transactions']
    assert summary['queue_peak']['events'] <= summary['queue_bounds']['events']
    assert summary['queue_peak']['bytes'] <= summary['queue_bounds']['bytes']
    assert summary['strict_buffer_peak']['events'] <= summary['strict_buffer_bounds']['events']
    assert summary['strict_buffer_peak']['bytes'] <= summary['strict_buffer_bounds']['bytes']
    assert summary['socket_buffers']['minimum_asserted'] is False
    assert summary['elapsed_throughput_events_per_sec'] > 0.0
    assert summary['failures'] == []

    strict = summary['strict_rejoin']
    assert strict['states'] == ['detached', 'catching-up', 'fixed-boundary-rejoin', 'running']
    assert strict['epoch_count'] >= 1
    assert strict['catchup_delivery_rate'] > 25.0
    assert strict['delivery_rate_margin'] > 0.0
    assert strict['retention_peak_events'] >= strict['measured_backlog_events']
    assert strict['retention_peak_bytes'] >= strict['retention_peak_events'] * 512
    assert strict['holdback_windows_paid'] == 1
    expected_bound = 2 * strict['measured_backlog_events'] / strict['delivery_rate_margin'] + 1.0
    bound = strict['derived_rejoin_bound_secs']
    elapsed = strict['measured_rejoin_secs']
    assert bound is not None
    assert elapsed is not None
    assert bound == pytest.approx(expected_bound)
    assert elapsed <= bound
    assert list(tmp_path.iterdir()) == []


def test_strict_rejoin_executes_fixed_boundary_event_time_trace(tmp_path: Path) -> None:
    summary = benchmark_state._run_benchmark(
        benchmark_state._BenchmarkConfig(
            rate=25.0,
            duration_secs=1.0,
            groups=1,
            match_ratio=1.0,
            callbacks_per_match=1,
            payload_bytes=128,
            numeric_gap_rate=0.0,
            ordering='strict',
            holdback_secs=1.0,
            sqlite_synchronous='normal',
            simulated_callback_latency_ms=10.0,
            simulated_storage_latency_ms=0.0,
            state_dir=tmp_path,
        )
    )

    strict = summary['strict_rejoin']
    assert strict['fixed_boundary_event_id'] == 25
    assert strict['epoch_page_sizes'] == [25]
    assert strict['offered_during_catchup'] > 0
    assert strict['retention_peak_events'] > 0
    assert strict['delivered_event_ids'] == strict['offered_event_ids']
    assert strict['final_cursor'] == strict['final_offered_event_id']
    assert strict['delivery_gap_count'] == 0
    assert strict['decreasing_id_count'] == 0
    assert strict['lag_events'] == 0
    assert strict['measured_rejoin_secs'] == strict['event_time_trace'][-1]['at_secs']
    tautological_elapsed = strict['measured_backlog_events'] / strict['delivery_rate_margin'] + 1.0
    assert strict['measured_rejoin_secs'] != pytest.approx(tautological_elapsed)
    bound = strict['derived_rejoin_bound_secs']
    elapsed = strict['measured_rejoin_secs']
    assert bound is not None
    assert elapsed is not None
    assert elapsed <= bound
    assert summary['failures'] == []
