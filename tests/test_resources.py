from __future__ import annotations

import pytest

from sg_event_daemon._resources import close_callables, close_resources


def test_close_callables_retains_later_failure_note_for_deferred_raise() -> None:
    initiating = OSError('selector close failed')

    def fail() -> None:
        raise RuntimeError('socket unlink failed')

    close_callables(
        (fail,),
        primary_error=initiating,
        failure_note_prefix='additional cleanup failure',
    )

    assert initiating.__notes__ == [
        "additional cleanup failure: RuntimeError('socket unlink failed')"
    ]


def test_close_callables_runs_all_actions_and_keeps_first_failure() -> None:
    attempts: list[str] = []
    first = OSError('unregister failed')
    later = KeyboardInterrupt('socket close failed')

    def attempt(name: str, failure: BaseException | None = None) -> None:
        attempts.append(name)
        if failure is not None:
            raise failure

    with pytest.raises(OSError, match='unregister failed') as caught:
        close_callables(
            (
                lambda: attempt('unregister', first),
                lambda: attempt('forget'),
                lambda: attempt('socket', later),
            )
        )

    assert caught.value is first
    assert attempts == ['unregister', 'forget', 'socket']
    assert any('socket close failed' in note for note in first.__notes__)


def test_close_resources_keeps_first_failure_and_notes_later_failures() -> None:
    attempts: list[str] = []
    first = OSError('database close failed')
    later = KeyboardInterrupt('lock release failed')

    class Resource:
        def __init__(self, name: str, failure: BaseException | None = None) -> None:
            self.name = name
            self.failure = failure

        def close(self) -> None:
            attempts.append(self.name)
            if self.failure is not None:
                raise self.failure

    resources = (Resource('database', first), None, Resource('handoff'), Resource('lock', later))

    with pytest.raises(OSError, match='database close failed') as caught:
        close_resources(resources)

    assert caught.value is first
    assert attempts == ['database', 'handoff', 'lock']
    assert any('lock release failed' in note for note in first.__notes__)


def test_close_resources_keeps_initiating_error_and_notes_every_failure() -> None:
    attempts: list[str] = []
    initiating = ValueError('open failed')

    class Resource:
        def __init__(self, name: str) -> None:
            self.name = name

        def close(self) -> None:
            attempts.append(self.name)
            raise OSError(f'{self.name} close failed')

    close_resources((Resource('database'), Resource('lock')), primary_error=initiating)

    assert attempts == ['database', 'lock']
    assert len(initiating.__notes__) == 2
    assert 'database close failed' in initiating.__notes__[0]
    assert 'lock close failed' in initiating.__notes__[1]
