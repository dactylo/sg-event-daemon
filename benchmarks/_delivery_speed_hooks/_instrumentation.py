"""Supply offline events and observe production publication and transactions."""

from __future__ import annotations

import atexit
import json
import os
import socket
import sys
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar, cast

from sg_event_daemon._fanout import EventDelivery, EventFanout
from sg_event_daemon._providers import Provider
from sg_event_daemon._write_lock import SourceWriteLock, WriteTiming

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Generator
    from contextlib import AbstractContextManager

    from sg_event_daemon._config import CredentialConfig
    from sg_event_daemon._fanout import FanoutMessage, GroupChannelInterface

_BASE_ID = 1_000_000
_F = TypeVar('_F', bound=Callable[..., object])


class _FakeHandle:
    """Answer only the daemon's EventLogEntry queries from an atomic release gate."""

    def __init__(self, directory: Path) -> None:
        """Keep the benchmark-owned release directory."""
        self._directory = directory

    def close(self) -> None:
        """Release this resource-free synthetic handle."""

    def find(
        self,
        entity_type: str,
        filters: object,
        fields: list[str] | None = None,
        **options: object,
    ) -> list[dict[str, object]]:
        """Return diagnosis payloads immediately, respecting query order and limits.

        Args:
            entity_type: Must be `EventLogEntry`.
            filters: Production ID filter expression.
            fields: An ID-only selection omits the remaining event payload.
            **options: Production limit, order, and page arguments.

        Raises:
            OSError: The benchmark's release-gate file cannot be read.
        """
        if entity_type != 'EventLogEntry':
            raise ValueError('offline provider accepts only EventLogEntry')
        count = int(json.loads((self._directory / 'available.json').read_text())['count'])
        identifiers = _identifiers(count, filters)
        if options.get('order') == [{'field_name': 'id', 'direction': 'desc'}]:
            identifiers.reverse()
        limit = options.get('limit', 0)
        page = options.get('page', 1)
        if not isinstance(limit, int) or not isinstance(page, int):
            raise TypeError('offline pagination requires integer limit and page')
        if limit:
            start = max(0, page - 1) * limit
            identifiers = identifiers[start : start + limit]
        return [
            (
                {'type': 'EventLogEntry', 'id': identifier}
                if fields == ['id']
                else _event(identifier)
            )
            for identifier in identifiers
        ]

    def set_session_uuid(self, session_uuid: str | None) -> None:
        """Accept the ordinary callback handle session lifecycle.

        Args:
            session_uuid: Synthetic callbacks do not need a provider session.
        """


def _event(identifier: int) -> dict[str, object]:
    """Construct the unchanged diagnosis event body."""
    shot_id = (identifier - _BASE_ID) // 4 + 1
    return {
        'type': 'EventLogEntry',
        'id': identifier,
        'event_type': 'Shotgun_Shot_New',
        'attribute_name': None,
        'meta': {
            'type': 'new_entity',
            'entity_type': 'Shot',
            'entity_id': shot_id,
            'new_value': 'offline-burst',
            'padding': 'x' * 128,
        },
        'entity': {'type': 'Shot', 'id': shot_id, 'name': 'offline-shot'},
        'user': None,
        'project': {'type': 'Project', 'id': 1, 'name': 'offline-project'},
        'session_uuid': None,
        'created_at': datetime(2026, 11, 4, tzinfo=UTC),
    }


def _matches_predicate(identifier: int, items: list[object]) -> bool:
    """Evaluate one ID comparison without changing the production query."""
    field, operator, *values = items
    if field != 'id':
        raise ValueError('offline filters must select id')
    value = values[0]
    if operator == 'in' and isinstance(value, list):
        return identifier in cast('list[object]', value)
    if not isinstance(value, int):
        raise TypeError('offline ID bound must be an integer')
    if operator == 'greater_than':
        return identifier > value
    if operator == 'less_than':
        return identifier < value
    if operator == 'is':
        return identifier == value
    if operator == 'between' and isinstance(values[1], int):
        return value <= identifier <= values[1]
    raise ValueError(f'unsupported offline filter: {operator}')


def _matches(identifier: int, filters: object) -> bool:
    """Evaluate only the production provider's supported ID predicates."""
    if isinstance(filters, dict):
        expression = cast('dict[str, object]', filters)
        nested = expression['filters']
        if not isinstance(nested, list):
            raise TypeError('invalid filter group')
        values = [_matches(identifier, item) for item in cast('list[object]', nested)]
        return any(values) if expression.get('filter_operator') == 'any' else all(values)
    if not isinstance(filters, list):
        raise TypeError('invalid filter expression')
    items = cast('list[object]', filters)
    if not items:
        return True
    if isinstance(items[0], (list, dict)):
        return all(_matches(identifier, item) for item in items)
    return _matches_predicate(identifier, items)


def _identifiers(count: int, filters: object) -> list[int]:
    """Retain the diagnosis's fast common forward-ID query path."""
    if isinstance(filters, list):
        clauses = cast('list[object]', filters)
        if len(clauses) == 1 and isinstance(clauses[0], list):
            clause = cast('list[object]', clauses[0])
            if clause[:2] == ['id', 'greater_than'] and isinstance(clause[2], int):
                return list(range(max(_BASE_ID, clause[2] + 1), _BASE_ID + count + 1))
    return [
        identifier
        for identifier in range(_BASE_ID, _BASE_ID + count + 1)
        if _matches(identifier, cast('object', filters))
    ]


def _observe_offer(
    original: _F,
    publications: list[dict[str, object]],
    final_id: int,
) -> _F:
    """Keep successful final-event enqueue times for each actual channel."""

    @wraps(original)
    def offer(channel: GroupChannelInterface, message: FanoutMessage) -> bool:
        accepted = original(channel, message)
        if not isinstance(accepted, bool):
            raise TypeError('channel offer did not return bool')
        if (
            accepted
            and isinstance(message, EventDelivery)
            and message.encoded.event_id == final_id
        ):
            publications.append(
                {
                    'group': channel.group,
                    'event_id': final_id,
                    'end': time.monotonic(),
                }
            )
        return accepted

    return cast('_F', offer)


def _observe_transaction(
    original: _F,
    transactions: list[dict[str, object]],
) -> _F:
    """Record disjoint write boundaries after the original context releases locks."""

    @wraps(original)
    @contextmanager
    def transaction(*args: object, **kwargs: object) -> Generator[sqlite3.Connection]:
        timing = kwargs.get('timing')
        if timing is None:
            timing = WriteTiming()
            kwargs['timing'] = timing
        if not isinstance(timing, WriteTiming):
            raise TypeError('unexpected write timing')
        committed = False
        try:
            with cast(
                'AbstractContextManager[sqlite3.Connection]', original(*args, **kwargs)
            ) as active:
                yield active
            committed = True
        finally:
            transactions.append(
                {
                    'thread': threading.current_thread().name,
                    'started_at': timing.started_at,
                    'begun_at': timing.begun_at,
                    'finished_at': timing.finished_at,
                    'committed': committed,
                }
            )

    return cast('_F', transaction)


def _offline_guard(event: str, arguments: tuple[object, ...]) -> None:
    """Permit Unix IPC while rejecting network connections and name resolution."""
    if event == 'socket.getaddrinfo':
        raise RuntimeError('offline benchmark forbids DNS')
    if event in {'socket.connect', 'socket.sendto', 'socket.sendmsg', 'socket.bind'}:
        sock = arguments[0]
        if isinstance(sock, socket.socket) and sock.family != socket.AF_UNIX:
            raise RuntimeError('offline benchmark forbids network sockets')


def install() -> None:
    """Install child-only offline guards, fake handles, and passive observations.

    Requires the benchmark-owned `DELIVERY_BENCHMARK_DIR` environment variable.
    No normal daemon configuration selects this provider. Redirect standard
    output and error to per-child log files, and give a directly launched
    supervisor its own session. Guards and patches remain installed for the
    process lifetime. Each process retains its installation marker immediately
    and its buffered observations on exit.

    Raises:
        OSError: Benchmark files, output descriptors, or the child session
            cannot be prepared.
        RuntimeError: The child inherited provider-credential environment
            variables.
    """
    directory = Path(os.environ['DELIVERY_BENCHMARK_DIR'])
    for descriptor, label in ((1, 'stdout'), (2, 'stderr')):
        with (directory / f'child-{os.getpid()}.{label}.log').open('wb') as stream:
            os.dup2(stream.fileno(), descriptor)
    own_session = (
        str(os.getppid()) == os.environ.get('DELIVERY_BENCHMARK_PARENT')
        and 'supervise' in sys.argv
    )
    if own_session:
        os.setsid()
    sys.addaudithook(_offline_guard)
    if any(name.startswith(('SG_', 'SHOTGUN_')) for name in os.environ):
        raise RuntimeError('benchmark child inherited provider credentials')
    metadata = json.loads((directory / 'run.json').read_text())
    final_id = _BASE_ID + int(metadata['warmup']) + int(metadata['burst'])
    publications: list[dict[str, object]] = []
    transactions: list[dict[str, object]] = []
    patched: set[type[GroupChannelInterface]] = set()
    original_attach = EventFanout.attach

    def create_sync(self: Provider, site: str, credential: CredentialConfig) -> _FakeHandle:
        del self, site, credential
        return _FakeHandle(directory)

    def attach(
        self: EventFanout, channel: GroupChannelInterface, *, provisional: bool = False
    ) -> None:
        channel_type = type(channel)
        if channel_type not in patched:
            # Patch the public protocol on its actual implementation, preserving
            # channel identity for promotion, fencing, and detachment.
            channel_type.offer = _observe_offer(channel_type.offer, publications, final_id)
            channel_type.offer_paced = _observe_offer(
                channel_type.offer_paced, publications, final_id
            )
            patched.add(channel_type)
        original_attach(self, channel, provisional=provisional)

    def dump() -> None:
        (directory / f'timings-{os.getpid()}.json').write_text(
            json.dumps(
                {
                    'pid': os.getpid(),
                    'publications': publications,
                    'transactions': transactions,
                }
            )
            + '\n'
        )

    Provider.create_sync = create_sync
    EventFanout.attach = attach
    SourceWriteLock.transaction = _observe_transaction(SourceWriteLock.transaction, transactions)
    atexit.register(dump)
    (directory / f'instrumented-{os.getpid()}.json').write_text(
        json.dumps(
            {
                'pid': os.getpid(),
                'parent_pid': os.getppid(),
                'argv': sys.argv,
                'monotonic': time.monotonic(),
                'wall': time.time(),
                'network_guard': True,
            }
        )
        + '\n'
    )
    if own_session:
        (directory / f'session-{os.getpid()}.json').write_text(
            json.dumps(
                {
                    'pid': os.getpid(),
                    'sid': os.getsid(0),
                }
            )
            + '\n'
        )
