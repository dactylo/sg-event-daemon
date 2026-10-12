from collections.abc import Callable
from typing import NotRequired, Protocol, TypedDict, Unpack


class _CallbackRegistration(TypedDict):
    id: str
    callback: Callable[[object], None]
    events: NotRequired[object | None]


class _Registrar(Protocol):
    def add_callback(
        self,
        **registration: Unpack[_CallbackRegistration],
    ) -> None: ...


def _noop(context: object) -> None:
    pass


def register(reg: _Registrar) -> None:
    reg.add_callback(id='noop', callback=_noop, events=None)
