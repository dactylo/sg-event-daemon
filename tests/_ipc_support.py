from __future__ import annotations

import os
import select
import socket
from typing import TYPE_CHECKING

from sg_event_daemon._ipc import encode_control_message

if TYPE_CHECKING:
    from pathlib import Path

    from sg_event_daemon._ipc import ControlMessage, WorkerControl


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
from sg_event_daemon._worker import _ComponentHealthCache, _WorkerRuntime

entered = threading.Event()
release = threading.Event()
forced = []
closed = []
runtime = None
target_control = None
clock = [0.0]

class StuckSource:
    def run(self):
        entered.set()
        release.wait()
    def request_stop(self, *, shutdown_deadline_at=None):
        pass
    def _request_checkpoint(self):
        pass
    def close(self):
        closed.append('source')
        raise AssertionError('closed resources still owned by stuck source')
    def execute_operator_command(self, command, *, wait_phase=None):
        raise AssertionError('unexpected operator command')

source = StuckSource()
def build(config, spec, assignment, control):
    global runtime, target_control
    runtime = _WorkerRuntime(
        spec=spec, spawn_instance=assignment.spawn_instance,
        health_cache=_ComponentHealthCache(site=spec.site, group_sources={}),
        source_factory=lambda name, slots, host: source,
        lane_factory=lambda name, slots, host: source,
        health_sink=lambda health: None,
        health_interval_secs=config.daemon.health_interval_secs,
        checkpoint_interval_secs=config.daemon.checkpoint_interval_secs,
        shutdown_deadline_secs=config.daemon.shutdown_deadline_secs,
        clock=lambda: clock[0], max_concurrent_catchup_readers=1,
        force_exit=lambda code: forced.append(threading.current_thread().name),
    )
    target_control = control
    return runtime

original_start = threading.Thread.start
def start(thread):
    if (
        runtime is not None
        and thread.name == 'sg-event-daemon:operator-executor'
        and getattr(thread, '_target', None) == runtime._operator_main
    ):
        entered.wait()
        runtime.request_stop('injected-startup-failure')
        clock[0] += runtime._shutdown_deadline_secs
        raise RuntimeError('injected operator startup failure')
    original_start(thread)

original_close = WorkerControl.close
def close(control):
    if runtime is not None and control is target_control:
        Path(report_path).write_text(json.dumps({
            'stuck': runtime._stuck_threads,
            'resources_open': runtime._resources_open,
            'monitor_started': vars(runtime)['_monitor_thread'] is not None,
            'forced': forced,
            'closed': closed,
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


def receive_control(control: WorkerControl) -> ControlMessage:
    while True:
        try:
            return control.receive(timeout=0)
        except TimeoutError:
            select.select((control.open_socket(),), (), ())


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


def send_control(control: WorkerControl, message: ControlMessage) -> None:
    frame = encode_control_message(message)
    assert len(frame) - 4 <= control.max_frame_bytes
    with control._send_lock:
        sock = control.open_socket()
        unsent = memoryview(frame)
        while unsent:
            try:
                sent = sock.send(unsent)
            except BlockingIOError:
                select.select((), (sock,), ())
                continue
            if sent == 0:
                raise EOFError('worker control peer closed')
            unsent = unsent[sent:]
