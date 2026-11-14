"""Measure worker socket capacity and cross-process monotonic clock agreement.

Run `socket-capacity --max-frame-bytes N` in the daemon environment, or run
`monotonic --processes 8 --rounds 1000` with standard-library Python alone.
Each command writes one JSON result. Clock disagreement exits nonzero.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing
import socket
import time
from dataclasses import asdict
from importlib import import_module
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from multiprocessing.connection import Connection
    from multiprocessing.process import BaseProcess

    from sg_event_daemon._ipc import WorkerControl


def _clock_child(connection: Connection) -> None:
    """Answer each parent request with this process's monotonic reading."""
    try:
        while connection.recv():
            connection.send(time.monotonic())
    finally:
        connection.close()


def _clock_reading(connection: Connection) -> float:
    """Receive one reading without letting a failed child hang the probe."""
    if not connection.poll(30):
        raise TimeoutError('clock child did not reply within 30 seconds')
    return float(connection.recv())


def _monotonic(processes: int, rounds: int) -> dict[str, object]:
    """Bracket all child readings and measure offsets from each bracket midpoint."""
    context = multiprocessing.get_context('spawn')
    children: list[BaseProcess] = []
    connections: list[Connection] = []
    largest_offset = 0.0
    largest_violation = 0.0
    try:
        for _ in range(processes):
            parent, child = context.Pipe()
            process = context.Process(target=_clock_child, args=(child,))
            process.start()
            child.close()
            connections.append(parent)
            children.append(process)
        for _ in range(rounds):
            before = time.monotonic()
            for connection in connections:
                connection.send('read')
            readings = [_clock_reading(connection) for connection in connections]
            after = time.monotonic()
            for reading in readings:
                largest_offset = max(largest_offset, abs(reading - (before + after) / 2))
                largest_violation = max(largest_violation, before - reading, reading - after)
        for connection in connections:
            connection.send(None)
    finally:
        for connection in connections:
            connection.close()
        for process in children:
            process.join(5)
            if process.is_alive():
                process.kill()
                process.join()
    return {
        'processes': processes,
        'rounds': rounds,
        'readings': processes * rounds,
        'passed': largest_violation == 0,
        'largest_offset_secs': largest_offset,
        'largest_violation_secs': largest_violation,
    }


def _positive_int(value: str) -> int:
    """Require positive probe dimensions."""
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError('must be positive')
    return parsed


def _socket_capacity(max_frame_bytes: int) -> dict[str, object]:
    """Fill a daemon-configured socket while its peer never reads."""
    control_type = cast('type[WorkerControl]', import_module('sg_event_daemon._ipc').WorkerControl)

    sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        control = control_type(sender, max_frame_bytes=max_frame_bytes)
        try:
            sender.settimeout(0.0)
            payload = bytes(65536)
            capacity = 0
            while True:
                try:
                    capacity += sender.send(payload)
                except BlockingIOError:
                    break
            return {
                'socket_buffers': asdict(control.socket_buffers),
                'payload_capacity_bytes': capacity,
            }
        finally:
            control.close()
    finally:
        sender.close()
        receiver.close()


def _main() -> int:
    """Run the selected host probe and report its result as JSON."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    capacity = commands.add_parser('socket-capacity')
    capacity.add_argument('--max-frame-bytes', type=_positive_int, required=True)
    monotonic = commands.add_parser('monotonic')
    monotonic.add_argument('--processes', type=_positive_int, default=8)
    monotonic.add_argument('--rounds', type=_positive_int, default=1000)
    args = parser.parse_args()
    if args.command == 'socket-capacity':
        result = _socket_capacity(args.max_frame_bytes)
    else:
        result = _monotonic(args.processes, args.rounds)
    print(json.dumps(result))
    return 1 if result.get('passed') is False else 0


if __name__ == '__main__':
    raise SystemExit(_main())
