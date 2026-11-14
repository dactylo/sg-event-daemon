from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Mapping
from contextvars import ContextVar
from importlib import import_module
from pathlib import Path
from traceback import extract_stack
from typing import cast

_CALLER: ContextVar[tuple[str, tuple[str, ...]] | None] = ContextVar(
    'source_query_caller', default=None
)


def _contains_between(value: object) -> bool:
    if not isinstance(value, (list, tuple)):
        return False
    return any(
        isinstance(item, (list, tuple))
        and len(cast('list[object]', item)) >= 2
        and cast('list[object]', item)[1] == 'between'
        for item in cast('list[object]', value)
    )


def _install_http_recorder(shotgun: type, path: Path, lock: threading.Lock) -> None:
    original_http = getattr(shotgun, '_http_request', None)
    if not callable(original_http):
        return
    http_path = path.with_suffix('.http.jsonl')

    def http_request(self: object, *args: object, **kwargs: object) -> object:
        started_at = time.time()
        started = time.monotonic()
        status: int | None = None
        error_name: str | None = None
        headers: dict[str, str] = {}
        try:
            result: object = original_http(self, *args, **kwargs)
            if isinstance(result, tuple):
                values = cast('tuple[object, ...]', result)
                if len(values) >= 2:
                    code = values[0]
                    if isinstance(code, tuple):
                        status_code = cast('tuple[object, ...]', code)
                        if status_code and type(status_code[0]) is int:
                            status = status_code[0]
                    response_headers = values[1]
                    if isinstance(response_headers, Mapping):
                        headers = {
                            key.lower(): str(value)
                            for key, value in cast(
                                'Mapping[object, object]', response_headers
                            ).items()
                            if isinstance(key, str)
                            and (
                                key.lower() == 'retry-after'
                                or key.lower().startswith('x-ratelimit-')
                            )
                        }
        except Exception as error:
            error_name = type(error).__name__
            raise
        else:
            return cast('object', result)
        finally:
            row = {
                'thread': (_CALLER.get() or (threading.current_thread().name, ()))[0],
                'started_at': started_at,
                'duration_secs': time.monotonic() - started,
                'status': status,
                'error': error_name,
                'headers': headers,
            }
            with lock, http_path.open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(row) + '\n')

    shotgun._http_request = http_request


def _is_between(value: object) -> bool:
    if isinstance(value, Mapping):
        mapping = cast('Mapping[str, object]', value)
        return mapping.get('filter_operator') == 'any' and _contains_between(
            mapping.get('filters')
        )
    if isinstance(value, (list, tuple)):
        return any(_is_between(item) for item in cast('list[object]', value))
    return False


def _response_ids(result: object) -> list[int]:
    if not isinstance(result, list):
        return []
    return [
        identifier
        for row in cast('list[object]', result)
        if isinstance(row, Mapping)
        and type(identifier := cast('Mapping[str, object]', row).get('id')) is int
    ]


def install() -> None:
    path = os.environ.get('SG_EVENT_TEST_REQUEST_LOG')
    if not path:
        return
    try:
        shotgun = import_module('shotgun_api3').Shotgun
    except ImportError:
        return
    source_module = import_module('sg_event_daemon._source')
    source_file = source_module.__file__
    original = shotgun.find
    lock = threading.Lock()

    def source_calls() -> tuple[str, ...]:
        return tuple(frame.name for frame in extract_stack() if frame.filename == source_file)

    def find(
        self: object, entity_type: str, filters: object, *args: object, **kwargs: object
    ) -> object:
        started_at = time.time()
        started = time.monotonic()
        result: object = None
        succeeded = False
        try:
            result = original(self, entity_type, filters, *args, **kwargs)
            succeeded = True
            return result
        finally:
            completed_at = time.time()
            duration_secs = time.monotonic() - started
            identifiers = _response_ids(result) if entity_type == 'EventLogEntry' else []
            caller = _CALLER.get()
            row = {
                'thread': (_CALLER.get() or (threading.current_thread().name, ()))[0],
                'between': entity_type == 'EventLogEntry' and _is_between(filters),
                'started_at': started_at,
                'duration_secs': duration_secs,
                'completed_at': completed_at,
                'succeeded': succeeded,
                'row_count': len(cast('list[object]', result))
                if isinstance(result, list)
                else None,
                'min_id': min(identifiers, default=None),
                'max_id': max(identifiers, default=None),
                'event_ids': identifiers,
                'source_calls': list(caller[1]) if caller is not None else list(source_calls()),
            }
            with lock, Path(path).open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(row) + '\n')

    shotgun.find = find
    servicing = getattr(source_module, '_ServicingSyncSG', None)
    if servicing is not None:
        original_servicing = servicing.find

        def source_find(self: object, *args: object, **kwargs: object) -> object:
            token = _CALLER.set(
                (
                    threading.current_thread().name,
                    source_calls(),
                )
            )
            try:
                return original_servicing(self, *args, **kwargs)
            finally:
                _CALLER.reset(token)

        servicing.find = source_find
    _install_http_recorder(shotgun, Path(path), lock)


if __name__ == 'sitecustomize':
    install()
