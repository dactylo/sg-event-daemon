import json
import subprocess
import sys
from pathlib import Path

_PROBE = Path(__file__).resolve().parents[1] / 'benchmarks' / 'host_probe.py'


def test_monotonic_probe_brackets_child_readings() -> None:
    # Isolated Python without site packages proves this mode needs only the standard library.
    child_code = (
        'import runpy, sys\n'
        'from multiprocessing.connection import Connection\n'
        'original_poll = Connection.poll\n'
        'def poll(self, timeout=0.0):\n'
        '    return original_poll(self, 3600 if timeout == 30 else timeout)\n'
        'Connection.poll = poll\n'
        f'sys.argv = [{str(_PROBE)!r}, *sys.argv[1:]]\n'
        f'runpy.run_path({str(_PROBE)!r}, run_name="__main__")\n'
    )
    result = subprocess.run(
        [
            sys.executable,
            '-I',
            '-S',
            '-c',
            child_code,
            'monotonic',
            '--processes',
            '3',
            '--rounds',
            '20',
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload['processes'] == 3
    assert payload['rounds'] == 20
    assert payload['readings'] == 60
    assert payload['passed'] is True
    assert payload['largest_violation_secs'] == 0
    assert payload['largest_offset_secs'] >= 0


def test_socket_capacity_reports_granted_sizes_and_capacity() -> None:
    result = subprocess.run(
        [sys.executable, str(_PROBE), 'socket-capacity', '--max-frame-bytes', '65536'],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload['socket_buffers']['requested'] == 65536
    assert payload['socket_buffers']['send'] > 0
    assert payload['socket_buffers']['receive'] > 0
    assert payload['payload_capacity_bytes'] > 0
