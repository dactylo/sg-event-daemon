from __future__ import annotations

import os
import socket
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from sg_event_daemon._ipc import WorkerControl


def partial_startup_child_code(config_path: Path, report_path: Path) -> str:
    return (
        f'config_path = {os.fspath(config_path)!r}\nreport_path = {os.fspath(report_path)!r}\n'
        """
import json
import sys
import threading
from pathlib import Path
import sg_event_daemon._cli as cli
from sg_event_daemon._ipc import WorkerControl
from sg_event_daemon._worker import WorkerRuntime

entered = threading.Event()
release = threading.Event()
forced = []
runtime = None

class StuckSource:
    def _run(self):
        entered.set()
        release.wait()
    def _request_stop(self, *, shutdown_deadline_at=None):
        pass
    def _request_checkpoint(self):
        pass
    def _close(self):
        raise AssertionError('closed resources still owned by stuck source')
    def _execute_operator_command(self, command):
        raise AssertionError('unexpected operator command')

source = StuckSource()
def build(config, spec, assignment, control):
    global runtime
    runtime = WorkerRuntime(
        spec=spec, spawn_instance=assignment.spawn_instance,
        source_factory=lambda name, slots, host: source,
        lane_factory=lambda name, slots, host: source,
        health_sink=lambda health: None,
        health_interval_secs=0.01, checkpoint_interval_secs=1.0,
        shutdown_deadline_secs=0.05, max_concurrent_catchup_readers=1,
        force_exit=lambda code: forced.append(threading.current_thread().name),
    )
    return runtime

original_start = threading.Thread.start
def start(thread):
    if thread.name == 'sg-event-daemon:operator-executor':
        assert entered.wait(2.0)
        raise RuntimeError('injected operator startup failure')
    original_start(thread)

original_close = WorkerControl.close
def close(control):
    if runtime is not None:
        Path(report_path).write_text(json.dumps({
            'stuck': runtime._stuck_threads,
            'resources_open': runtime._resources_open,
            'monitor_started': vars(runtime)['_monitor_thread'] is not None,
            'forced': forced,
        }))
    original_close(control)

cli.build_worker_runtime = build
threading.Thread.start = start
WorkerControl.close = close
result = cli.main([
    'run-worker', '--config', config_path, '--site', 'test',
    '--worker', sys.argv[3], '--control-fd', sys.argv[1],
    '--spawn-instance', sys.argv[2],
])
assert result == 1
"""
    )


def saturate_worker_control_sender(control: WorkerControl) -> None:
    sock = control._socket
    assert sock is not None
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4_096)
    payload = memoryview(b'x' * 65_536)
    while True:
        try:
            sent = sock.send(payload)
        except BlockingIOError:
            return
        assert sent > 0
