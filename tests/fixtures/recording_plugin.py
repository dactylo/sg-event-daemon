from collections.abc import Callable
from pathlib import Path
from typing import NotRequired, Protocol, TypedDict, Unpack

from sg_event_daemon._plugins import EventContext


class _Registration(TypedDict):
    id: str
    callback: Callable[[EventContext], None]
    events: NotRequired[object | None]


class _Registrar(Protocol):
    def add_callback(self, **registration: Unpack[_Registration]) -> None: ...


def _record(context: EventContext) -> None:
    filename = context.config['record_file']
    assert isinstance(filename, str)
    with Path(filename).open('a', encoding='utf-8') as stream:
        stream.write(f'{context.group} {context.event["id"]}\n')
        stream.flush()


def register(reg: _Registrar) -> None:
    reg.add_callback(id='record', callback=_record)
