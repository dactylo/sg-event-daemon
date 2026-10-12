"""Secure SQLite connection, schema, transaction, and checkpoint primitives."""

from __future__ import annotations

import math
import os
import sqlite3
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from contextlib import AbstractContextManager
    from types import TracebackType
    from typing import Self

from sg_event_daemon._platform import secure_open_regular_file

_WriteTransaction: TypeAlias = (
    'Callable[[sqlite3.Connection], AbstractContextManager[sqlite3.Connection]]'
)

BUSY_TIMEOUT_SECS = 5.0
_MAX_BUSY_TIMEOUT_MILLIS = 2_147_483_647
_MIN_SQLITE_VERSION = (3, 51, 3)
_SYNCHRONOUS_LEVELS = frozenset({'FULL', 'NORMAL'})


class DatabaseError(RuntimeError):
    """Report an unusable database operation or policy violation."""


@dataclass
class DatabaseHandle:
    """Own one configured SQLite connection.

    The caller may use the handle as a context manager. `close()` or context
    exit closes the owned `connection`; database operations require that open
    lifetime.
    """

    connection: sqlite3.Connection
    path: Path

    def __enter__(self) -> Self:
        """Return this open database handle.

        Returns:
            This handle while its connection remains open.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the connection when leaving a context."""
        self.close()

    def close(self) -> None:
        """Close the owned SQLite connection."""
        self.connection.close()


@dataclass(frozen=True)
class Schema:
    """Define one database's complete current schema.

    Fresh initialization executes `statements` in one transaction and records
    `version`. Existing databases must match that version.

    Attributes:
        version: Supported schema version recorded after initialization.
        statements: Statements that create the complete schema at `version`.
    """

    version: int
    statements: tuple[str, ...]


class _SchemaVersionError(DatabaseError):
    """Report an unsupported or invalid database schema version."""


@dataclass(frozen=True)
class _OpenSettings:
    """Hold validated connection policy and file-creation settings."""

    busy_timeout_millis: int
    busy_timeout_secs: float
    create: bool
    write_transaction: _WriteTransaction | None
    synchronous: str


def _apply_connection_policy(
    connection: sqlite3.Connection,
    synchronous: str,
    busy_timeout_millis: int,
) -> None:
    """Apply the complete policy required on every database connection."""
    journal_row = connection.execute('PRAGMA journal_mode=WAL').fetchone()
    if journal_row is None or str(journal_row[0]).casefold() != 'wal':
        raise DatabaseError('SQLite did not enable required WAL journal mode')
    connection.execute('PRAGMA foreign_keys=ON')
    connection.execute(f'PRAGMA synchronous={synchronous}')
    connection.execute('PRAGMA wal_autocheckpoint=0')
    connection.execute(f'PRAGMA busy_timeout={busy_timeout_millis}')


def _cleanup_new_database(path: Path) -> None:
    """Remove files created by a failed first-time initialization."""
    for suffix in ('-shm', '-wal', ''):
        candidate = Path(f'{path}{suffix}')
        with suppress(FileNotFoundError):
            candidate.unlink()


def _milliseconds(seconds: float) -> int:
    """Convert a finite positive timeout to bounded SQLite milliseconds."""
    if not math.isfinite(seconds) or seconds <= 0:
        raise DatabaseError('busy_timeout_secs must be a finite positive number')
    milliseconds = round(seconds * 1000)
    if milliseconds <= 0 or milliseconds > _MAX_BUSY_TIMEOUT_MILLIS:
        raise DatabaseError('busy_timeout_secs is outside SQLite timeout bounds')
    return milliseconds


def _open_settings(
    synchronous: str,
    *,
    busy_timeout_secs: float,
    create: bool,
    write_transaction: _WriteTransaction | None,
) -> _OpenSettings:
    """Validate opening policy and derive SQLite connection settings."""
    if synchronous not in _SYNCHRONOUS_LEVELS:
        raise DatabaseError('synchronous must be NORMAL or FULL')
    return _OpenSettings(
        busy_timeout_millis=_milliseconds(busy_timeout_secs),
        busy_timeout_secs=busy_timeout_secs,
        create=create,
        synchronous=synchronous,
        write_transaction=write_transaction,
    )


@contextmanager
def _transaction_scope(
    connection: sqlite3.Connection, begin_statement: str
) -> Generator[sqlite3.Connection]:
    """Run the shared SQLite transaction lifecycle for one begin mode."""
    try:
        connection.execute(begin_statement)
        try:
            yield connection
        except sqlite3.Error as exc:
            connection.rollback()
            raise DatabaseError(str(exc)) from exc
        except BaseException:
            connection.rollback()
            raise
        try:
            connection.commit()
        except sqlite3.Error as exc:
            connection.rollback()
            raise DatabaseError(str(exc)) from exc
    except sqlite3.Error as exc:
        raise DatabaseError(str(exc)) from exc


def _validate_schema(schema: Schema) -> None:
    """Reject invalid schema declarations before opening a database."""
    if schema.version <= 0:
        raise _SchemaVersionError('schema version must be positive')


def _validate_sqlite_version() -> None:
    """Reject SQLite libraries predating the required WAL corruption fix."""
    if sqlite3.sqlite_version_info < _MIN_SQLITE_VERSION:
        required = '.'.join(str(part) for part in _MIN_SQLITE_VERSION)
        actual = sqlite3.sqlite_version
        raise DatabaseError(f'SQLite {required} or newer is required; runtime provides {actual}')


def database_family_size(path: Path) -> int:
    """Return the current bytes used by one SQLite file family.

    Args:
        path: Main SQLite database path.

    Returns:
        Sum of the main file and existing WAL and SHM sidecars.
    """
    total = 0
    for candidate in (path, Path(f'{path}-wal'), Path(f'{path}-shm')):
        with suppress(FileNotFoundError):
            total += candidate.stat().st_size
    return total


@contextmanager
def read_transaction(connection: sqlite3.Connection) -> Generator[sqlite3.Connection]:
    """Run one deferred SQLite read transaction without taking a write lock.

    Args:
        connection: Configured SQLite connection.

    Yields:
        The connection while its read snapshot is active.

    Raises:
        DatabaseError: If SQLite cannot begin, execute, or commit the
            transaction.
    """
    with _transaction_scope(connection, 'BEGIN'):
        yield connection


@contextmanager
def transaction(connection: sqlite3.Connection) -> Generator[sqlite3.Connection]:
    """Run one short immediate SQLite transaction for supervisor.db.

    Args:
        connection: Configured SQLite connection.

    Yields:
        The connection while its immediate transaction is active.

    Raises:
        DatabaseError: If SQLite cannot begin, execute, or commit the
            transaction.
    """
    with _transaction_scope(connection, 'BEGIN IMMEDIATE'):
        yield connection


def _initialize_database(
    connection: sqlite3.Connection,
    identity: str,
    schema: Schema,
    write_transaction: _WriteTransaction | None,
) -> None:
    """Create metadata and the first schema in one transaction."""
    initialize = transaction if write_transaction is None else write_transaction
    with initialize(connection):
        connection.execute(
            'CREATE TABLE _daemon_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)'
        )
        connection.execute(
            "INSERT INTO _daemon_metadata VALUES ('identity', ?)",
            (identity,),
        )
        for statement in schema.statements:
            connection.execute(statement)
        connection.execute(f'PRAGMA user_version={schema.version}')


def _initialize_or_validate(
    connection: sqlite3.Connection,
    identity: str,
    schema: Schema,
    *,
    settings: _OpenSettings,
    path: Path,
) -> None:
    """Initialize a fresh database or validate its identity and schema version."""
    current_version = int(connection.execute('PRAGMA user_version').fetchone()[0])
    has_metadata = bool(
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_daemon_metadata'"
        ).fetchone()
    )
    if current_version == 0 and not has_metadata:
        if not settings.create:
            raise DatabaseError(f'database is not initialized: {path}')
        _initialize_database(connection, identity, schema, settings.write_transaction)
        return
    if not settings.create and (current_version == 0 or not has_metadata):
        raise DatabaseError(f'database is not initialized: {path}')
    if not has_metadata:
        raise _SchemaVersionError('database is missing identity metadata')
    stored_row = connection.execute(
        "SELECT value FROM _daemon_metadata WHERE key = 'identity'"
    ).fetchone()
    if stored_row is None:
        if not settings.create:
            raise DatabaseError(f'database is not initialized: {path}')
        raise _SchemaVersionError('database is missing identity metadata')
    stored_identity = str(stored_row[0])
    if stored_identity != identity:
        raise DatabaseError(
            f'database identity mismatch: stored {stored_identity!r}, requested {identity!r}'
        )
    if current_version > schema.version:
        raise _SchemaVersionError(
            f'database has newer schema version {current_version}; '
            f'this release supports {schema.version}'
        )
    if current_version != schema.version:
        raise _SchemaVersionError(
            f'database has unsupported schema version {current_version}; '
            f'this release supports {schema.version}'
        )


def _configure_open_connection(
    connection: sqlite3.Connection,
    path: Path,
    identity: str,
    schema: Schema,
    settings: _OpenSettings,
) -> None:
    """Validate existing content before applying policy or initialize a new file."""
    if not settings.create:
        _initialize_or_validate(connection, identity, schema, settings=settings, path=path)
    _apply_connection_policy(connection, settings.synchronous, settings.busy_timeout_millis)
    if settings.create:
        _initialize_or_validate(connection, identity, schema, settings=settings, path=path)


def open_database(  # noqa: PLR0913
    path: Path,
    identity: str,
    schema: Schema,
    *,
    synchronous: str,
    busy_timeout_secs: float = BUSY_TIMEOUT_SECS,
    create: bool = True,
    write_transaction: _WriteTransaction | None = None,
) -> DatabaseHandle:
    """Open and configure one secure daemon SQLite database.

    Args:
        path: Database path beneath an owner-only state directory.
        identity: Stable semantic identity stored in the database.
        schema: Complete initial schema and supported version.
        synchronous: SQLite synchronous policy, `NORMAL` or `FULL`.
        busy_timeout_secs: Finite positive lock wait bound.
        create: Whether to initialize a missing database.
        write_transaction: Optional initialization transaction factory; `None`
            resolves the supervisor transaction helper at call time.

    Returns:
        An owned configured database handle.

    Raises:
        SecurityError: The database path or its parent cannot be securely
            opened or fails filesystem safety checks.
        DatabaseError: If policy, opening, initialization, or validation fails,
            including when the schema version is unsupported.
    """
    _validate_sqlite_version()
    _validate_schema(schema)
    settings = _open_settings(
        synchronous,
        busy_timeout_secs=busy_timeout_secs,
        create=create,
        write_transaction=write_transaction,
    )
    try:
        fd, created = secure_open_regular_file(path, create=settings.create)
    except FileNotFoundError as exc:
        if settings.create:
            raise
        raise DatabaseError(f'database does not exist: {path}') from exc
    os.close(fd)
    connection: sqlite3.Connection | None = None
    try:
        target = path if settings.create else path.absolute().as_uri() + '?mode=rw'
        connection = sqlite3.connect(
            target,
            isolation_level=None,
            timeout=settings.busy_timeout_secs,
            uri=not settings.create,
        )
        _configure_open_connection(connection, path, identity, schema, settings)
    except sqlite3.Error as exc:
        if connection is not None:
            connection.close()
        if settings.create and created:
            _cleanup_new_database(path)
        raise DatabaseError(str(exc)) from exc
    except BaseException:
        if connection is not None:
            connection.close()
        if settings.create and created:
            _cleanup_new_database(path)
        raise
    return DatabaseHandle(connection=connection, path=path)
