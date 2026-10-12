"""Exceptions for durable group state."""


class InvalidTransitionError(RuntimeError):
    """Report a rejected state transition that made no durable change."""


class RevisionConflictError(RuntimeError):
    """Report a stale expected group-state revision."""


class TerminalLateEventError(InvalidTransitionError):
    """Report a late event that cannot start or complete its pending reservation."""


class StrictObligationBlockedError(InvalidTransitionError):
    """Defer ordinary strict progress behind a pending durable obligation."""
