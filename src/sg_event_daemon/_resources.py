"""Shared synchronous resource cleanup with first-error preservation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable


class Closeable(Protocol):
    """Support resource cleanup through a synchronous close operation."""

    def close(self) -> None:
        """Release the owned resource."""


def close_callables(
    cleanups: Iterable[Callable[[], None]],
    *,
    primary_error: BaseException | None = None,
    failure_note_prefix: str | None = None,
) -> None:
    """Run each cleanup and preserve the initiating or first cleanup failure.

    With `primary_error`, attach cleanup failures as notes and return so the
    caller can raise that error after finishing its own state transitions.

    Args:
        cleanups: Actions to attempt in ownership order.
        primary_error: Existing failure that takes precedence over cleanup.
        failure_note_prefix: Label for later failures when preserving an
            existing error; the default follows `primary_error` semantics.
    """
    first_error = primary_error
    note_prefix = failure_note_prefix or (
        'cleanup failure' if primary_error is not None else 'additional cleanup failure'
    )
    for cleanup in cleanups:
        try:
            cleanup()
        except BaseException as error:  # noqa: BLE001
            if first_error is None:
                first_error = error
            else:
                first_error.add_note(f'{note_prefix}: {error!r}')
    if primary_error is None and first_error is not None:
        raise first_error


def close_resources(
    resources: Iterable[Closeable | None],
    *,
    primary_error: BaseException | None = None,
) -> None:
    """Close each resource while retaining the first failure and later notes.

    Args:
        resources: Owned resources in cleanup order; missing resources are
            skipped.
        primary_error: Existing failure to preserve and annotate.
    """
    close_callables(
        (resource.close for resource in resources if resource is not None),
        primary_error=primary_error,
    )
