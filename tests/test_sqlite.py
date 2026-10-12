from __future__ import annotations

import os
import select
import signal
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock

import pytest

import sg_event_daemon._sqlite as sqlite_module
from sg_event_daemon._sqlite import (
    _MIN_SQLITE_VERSION,
    BUSY_TIMEOUT_SECS,
    DatabaseError,
    Schema,
    _SchemaVersionError,
    open_database,
    transaction,
)
from tests._subprocess_support import subprocess_env

_SCHEMA = Schema(
    version=1,
    statements=('CREATE TABLE entries (id INTEGER PRIMARY KEY, value BLOB NOT NULL)',),
)


def _mock_connection_with_journal_mode(
    monkeypatch: pytest.MonkeyPatch,
    journal_row: tuple[str] | None,
) -> Mock:
    connection = Mock(spec=sqlite3.Connection)

    def connect(*_args: object, **_kwargs: object) -> Mock:
        return connection

    def execute(statement: str, _parameters: object = None) -> Mock:
        cursor = Mock(spec=sqlite3.Cursor)
        if statement == 'PRAGMA journal_mode=WAL':
            cursor.fetchone.return_value = journal_row
        elif statement == 'PRAGMA user_version':
            cursor.fetchone.return_value = (0,)
        elif statement.startswith('SELECT 1 FROM sqlite_master'):
            cursor.fetchone.return_value = None
        return cursor

    connection.execute.side_effect = execute
    monkeypatch.setattr(sqlite3, 'connect', connect)
    return connection


def _read_child_notice(process: subprocess.Popen[str], expected: str) -> None:
    stdout = process.stdout
    assert stdout is not None
    deadline = time.monotonic() + 10.0
    readable, _writable, _exceptional = select.select(
        (stdout,),
        (),
        (),
        max(0.0, deadline - time.monotonic()),
    )
    assert readable, f'child did not announce {expected} before deadline'
    assert os.read(stdout.fileno(), 4_096).decode().strip() == expected


def _reap_child(process: subprocess.Popen[str]) -> int:
    try:
        try:
            if process.poll() is None:
                process.kill()
        except ProcessLookupError:
            pass
        return process.wait(timeout=5.0)
    finally:
        if process.stdin is not None:
            process.stdin.close()
        if process.stdout is not None:
            process.stdout.close()


def _kill_child_at_database_point(path: Path, point: str) -> int:
    code = """
import os
import sqlite3
import sys
from pathlib import Path
from sg_event_daemon._sqlite import Schema, open_database, transaction

schema = Schema(
    version=1,
    statements=('CREATE TABLE entries (id INTEGER PRIMARY KEY, value BLOB NOT NULL)',),
)
handle = open_database(Path(sys.argv[1]), 'kill-point', schema, synchronous='NORMAL')
point = sys.argv[2]
if point == 'before-commit':
    handle.connection.execute('BEGIN IMMEDIATE')
    handle.connection.execute("INSERT INTO entries VALUES (1, X'01')")
    print('READY', flush=True)
elif point == 'after-commit':
    with transaction(handle.connection):
        handle.connection.execute("INSERT INTO entries VALUES (1, X'01')")
    print('READY', flush=True)
else:
    raise AssertionError(point)
sys.stdin.read()
os._exit(9)
"""
    process = subprocess.Popen(
        [sys.executable, '-c', code, str(path), point],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env=subprocess_env(),
    )
    try:
        _read_child_notice(process, 'READY')
        process.kill()
    finally:
        returncode = _reap_child(process)
    return returncode


def _rows(path: Path) -> list[tuple[int, bytes]]:
    handle = open_database(path, 'kill-point', _SCHEMA, synchronous='NORMAL')
    try:
        rows = handle.connection.execute('SELECT id, value FROM entries ORDER BY id').fetchall()
        return [(int(row[0]), bytes(row[1])) for row in rows]
    finally:
        handle.close()


def test_database_constants_match_settled_policy() -> None:
    assert BUSY_TIMEOUT_SECS == 5.0
    assert _MIN_SQLITE_VERSION == (3, 51, 3)


def test_database_handle_closes_connection(tmp_path: Path) -> None:
    with open_database(
        tmp_path / 'state.db',
        'close-test',
        _SCHEMA,
        synchronous='NORMAL',
    ) as handle:
        handle.connection.execute('SELECT 1')

    with pytest.raises(sqlite3.ProgrammingError, match='closed database'):
        handle.connection.execute('SELECT 1')


def test_database_initialization_failure_cleans_new_files(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    invalid_schema = Schema(
        version=1,
        statements=('CREATE TABLE good (id INTEGER)', 'THIS IS NOT SQL'),
    )

    with pytest.raises(DatabaseError):
        open_database(path, 'broken', invalid_schema, synchronous='NORMAL')

    assert not path.exists()
    assert not Path(f'{path}-wal').exists()
    assert not Path(f'{path}-shm').exists()


def test_database_rejects_identity_mismatch(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    open_database(path, 'first', _SCHEMA, synchronous='NORMAL').close()

    with pytest.raises(DatabaseError, match=r'identity.*first.*second'):
        open_database(path, 'second', _SCHEMA, synchronous='NORMAL')


def test_database_rejects_newer_schema(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    newer = Schema(version=2, statements=_SCHEMA.statements)
    open_database(path, 'schema-test', newer, synchronous='NORMAL').close()

    with pytest.raises(_SchemaVersionError, match='newer schema version 2'):
        open_database(path, 'schema-test', _SCHEMA, synchronous='NORMAL')


def test_database_rejects_old_sqlite_before_open(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / 'state.db'
    monkeypatch.setattr(sqlite3, 'sqlite_version_info', (3, 51, 2))

    with pytest.raises(DatabaseError, match=r'SQLite 3\.51\.3 or newer'):
        open_database(path, 'version-test', _SCHEMA, synchronous='NORMAL')

    assert not path.exists()


@pytest.mark.parametrize(
    ('journal_row', 'accepted'),
    [
        pytest.param(('delete',), False, id='delete'),
        pytest.param(('truncate',), False, id='other-mode'),
        pytest.param(None, False, id='no-row'),
        pytest.param(('WaL',), True, id='mixed-case-wal'),
    ],
)
def test_database_requires_effective_wal_mode(
    accepted: bool,
    journal_row: tuple[str] | None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    connection = _mock_connection_with_journal_mode(monkeypatch, journal_row)
    path = tmp_path / 'state.db'

    if not accepted:
        with pytest.raises(DatabaseError):
            open_database(path, 'wal-test', _SCHEMA, synchronous='NORMAL')
        assert not path.exists()
        return

    handle = open_database(path, 'wal-test', _SCHEMA, synchronous='NORMAL')
    try:
        assert handle.connection is connection
    finally:
        handle.close()


def test_database_rollback_translates_real_disk_full(tmp_path: Path) -> None:
    handle = open_database(tmp_path / 'state.db', 'full-test', _SCHEMA, synchronous='NORMAL')
    try:
        page_count = int(handle.connection.execute('PRAGMA page_count').fetchone()[0])
        handle.connection.execute(f'PRAGMA max_page_count={page_count + 1}')

        with (
            pytest.raises(DatabaseError, match='database or disk is full'),
            transaction(handle.connection),
        ):
            handle.connection.execute(
                'INSERT INTO entries VALUES (?, ?)',
                (1, b'x' * 1_000_000),
            )

        assert handle.connection.execute('SELECT COUNT(*) FROM entries').fetchone() == (0,)
    finally:
        handle.close()


def test_database_size_sums_only_existing_sqlite_family_files(tmp_path: Path) -> None:
    path = tmp_path / 'source.db'
    path.write_bytes(b'main')
    wal_path = path.with_name(f'{path.name}-wal')
    wal_path.write_bytes(b'wal-bytes')
    shm_path = path.with_name(f'{path.name}-shm')
    shm_path.write_bytes(b'shm')

    assert sqlite_module.database_family_size(path) == 4 + 9 + 3

    wal_path.unlink()

    assert sqlite_module.database_family_size(path) == 4 + 3


@pytest.mark.parametrize(('synchronous', 'expected'), [('NORMAL', 1), ('FULL', 2)])
def test_database_uses_required_pragmas(
    expected: int,
    synchronous: str,
    tmp_path: Path,
) -> None:
    handle = open_database(
        tmp_path / f'{synchronous.lower()}.db',
        f'pragma-{synchronous.lower()}',
        _SCHEMA,
        synchronous=synchronous,
        busy_timeout_secs=0.125,
    )
    try:
        connection = handle.connection
        assert connection.execute('PRAGMA journal_mode').fetchone() == ('wal',)
        assert connection.execute('PRAGMA foreign_keys').fetchone() == (1,)
        assert connection.execute('PRAGMA synchronous').fetchone() == (expected,)
        assert connection.execute('PRAGMA wal_autocheckpoint').fetchone() == (0,)
        assert connection.execute('PRAGMA busy_timeout').fetchone() == (125,)
        assert connection.execute(
            "SELECT value FROM _daemon_metadata WHERE key = 'identity'"
        ).fetchone() == (f'pragma-{synchronous.lower()}',)
        assert connection.execute('PRAGMA user_version').fetchone() == (1,)
    finally:
        handle.close()


def test_database_uses_secure_file_mode(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    handle = open_database(path, 'mode-test', _SCHEMA, synchronous='NORMAL')
    try:
        with transaction(handle.connection):
            handle.connection.execute("INSERT INTO entries VALUES (1, X'01')")

        for candidate in (path, Path(f'{path}-wal'), Path(f'{path}-shm')):
            assert candidate.stat().st_mode & 0o777 == 0o600
            assert candidate.stat().st_uid == os.geteuid()
    finally:
        handle.close()


def test_existing_only_open_applies_policy_to_initialized_database(tmp_path: Path) -> None:
    path = tmp_path / 'initialized.db'
    open_database(path, 'existing', _SCHEMA, synchronous='NORMAL').close()

    with open_database(
        path,
        'existing',
        _SCHEMA,
        synchronous='FULL',
        busy_timeout_secs=0.125,
        create=False,
    ) as handle:
        connection = handle.connection
        assert connection.execute('PRAGMA journal_mode').fetchone() == ('wal',)
        assert connection.execute('PRAGMA foreign_keys').fetchone() == (1,)
        assert connection.execute('PRAGMA synchronous').fetchone() == (2,)
        assert connection.execute('PRAGMA wal_autocheckpoint').fetchone() == (0,)
        assert connection.execute('PRAGMA busy_timeout').fetchone() == (125,)


def test_existing_only_open_leaves_uninitialized_file_unchanged(tmp_path: Path) -> None:
    path = tmp_path / 'empty.db'
    path.touch(mode=0o600)
    before = path.read_bytes(), path.stat().st_mode & 0o777

    with pytest.raises(DatabaseError, match=f'database is not initialized: {path}'):
        open_database(path, 'empty', _SCHEMA, synchronous='NORMAL', create=False)

    assert (path.read_bytes(), path.stat().st_mode & 0o777) == before


def test_existing_only_open_rejects_missing_file_without_creating(tmp_path: Path) -> None:
    path = tmp_path / 'missing.db'

    with pytest.raises(DatabaseError, match=f'database does not exist: {path}'):
        open_database(path, 'missing', _SCHEMA, synchronous='NORMAL', create=False)

    assert not path.exists()
    assert not Path(f'{path}-wal').exists()
    assert not Path(f'{path}-shm').exists()


def test_failed_existing_only_open_never_unlinks(tmp_path: Path) -> None:
    path = tmp_path / 'existing.db'
    open_database(path, 'first', _SCHEMA, synchronous='NORMAL').close()

    with pytest.raises(DatabaseError, match='identity mismatch'):
        open_database(path, 'second', _SCHEMA, synchronous='NORMAL', create=False)

    assert path.exists()


def test_full_handoff_uses_full_synchronous_and_committed_writes_are_visible(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'state.db'
    normal = open_database(path, 'handoff', _SCHEMA, synchronous='NORMAL')
    full = open_database(path, 'handoff', _SCHEMA, synchronous='FULL')
    observer = sqlite3.connect(path, isolation_level=None)
    try:
        assert normal.connection.execute('PRAGMA journal_mode').fetchone() == ('wal',)
        assert normal.connection.execute('PRAGMA synchronous').fetchone() == (1,)
        assert normal.connection.execute('PRAGMA busy_timeout').fetchone() == (5000,)
        assert full.connection.execute('PRAGMA journal_mode').fetchone() == ('wal',)
        assert full.connection.execute('PRAGMA synchronous').fetchone() == (2,)
        assert full.connection.execute('PRAGMA busy_timeout').fetchone() == (5000,)
        assert full.connection.execute('PRAGMA wal_autocheckpoint').fetchone() == (0,)

        with transaction(full.connection):
            full.connection.execute("INSERT INTO entries VALUES (1, X'01')")
        assert observer.execute('SELECT COUNT(*) FROM entries').fetchone() == (1,)
    finally:
        observer.close()
        full.close()
        normal.close()


def test_normal_and_full_writes_obey_one_database_mutex(tmp_path: Path) -> None:
    path = tmp_path / 'state.db'
    normal = open_database(
        path,
        'mutex',
        _SCHEMA,
        synchronous='NORMAL',
        busy_timeout_secs=0.01,
    )
    full = open_database(
        path,
        'mutex',
        _SCHEMA,
        synchronous='FULL',
        busy_timeout_secs=0.01,
    )
    try:
        normal.connection.execute('BEGIN IMMEDIATE')
        normal.connection.execute("INSERT INTO entries VALUES (1, X'01')")
        with pytest.raises(DatabaseError, match='locked'), transaction(full.connection):
            full.connection.execute("INSERT INTO entries VALUES (2, X'02')")
        normal.connection.commit()

        with transaction(full.connection):
            full.connection.execute("INSERT INTO entries VALUES (2, X'02')")
        assert normal.connection.execute('SELECT COUNT(*) FROM entries').fetchone() == (2,)
    finally:
        full.close()
        normal.close()


@pytest.mark.parametrize(
    ('point', 'expected_rows'),
    [('before-commit', []), ('after-commit', [(1, b'\x01')])],
)
def test_process_kill_points_preserve_commit_boundary(
    expected_rows: list[tuple[int, bytes]],
    point: str,
    tmp_path: Path,
) -> None:
    path = tmp_path / f'{point}.db'

    assert _kill_child_at_database_point(path, point) == -signal.SIGKILL

    assert _rows(path) == expected_rows
    with closing(sqlite3.connect(path)) as connection, connection:
        assert connection.execute('PRAGMA integrity_check').fetchone() == ('ok',)


def test_read_transaction_does_not_block_a_writer(tmp_path: Path) -> None:
    path = tmp_path / 'writer.db'
    with open_database(path, 'writer', _SCHEMA, synchronous='NORMAL') as reader:
        writer = sqlite3.connect(path, isolation_level=None, timeout=0)
        try:
            with sqlite_module.read_transaction(reader.connection):
                assert reader.connection.execute('SELECT COUNT(*) FROM entries').fetchone() == (0,)
                writer.execute('BEGIN IMMEDIATE')
                writer.execute("INSERT INTO entries VALUES (1, X'01')")
                writer.commit()
        finally:
            writer.close()


def test_read_transaction_reads_one_snapshot(tmp_path: Path) -> None:
    path = tmp_path / 'snapshot.db'
    with (
        open_database(path, 'snapshot', _SCHEMA, synchronous='NORMAL') as reader,
        open_database(path, 'snapshot', _SCHEMA, synchronous='NORMAL') as writer,
    ):
        with sqlite_module.read_transaction(reader.connection):
            assert reader.connection.execute('SELECT COUNT(*) FROM entries').fetchone() == (0,)
            with transaction(writer.connection):
                writer.connection.execute("INSERT INTO entries VALUES (1, X'01')")
            assert reader.connection.execute('SELECT COUNT(*) FROM entries').fetchone() == (0,)
        assert reader.connection.execute('SELECT COUNT(*) FROM entries').fetchone() == (1,)


def test_read_transaction_rolls_back_and_wraps_errors(tmp_path: Path) -> None:
    with open_database(
        tmp_path / 'rollback.db', 'rollback', _SCHEMA, synchronous='NORMAL'
    ) as handle:
        connection = handle.connection
        with pytest.raises(RuntimeError, match='stop'), sqlite_module.read_transaction(connection):
            raise RuntimeError('stop')
        assert not connection.in_transaction

        with (
            pytest.raises(DatabaseError, match='no such table'),
            sqlite_module.read_transaction(connection),
        ):
            connection.execute('SELECT * FROM missing_table')
        assert not connection.in_transaction


def test_transaction_rolls_back_on_exception(tmp_path: Path) -> None:
    handle = open_database(tmp_path / 'state.db', 'rollback', _SCHEMA, synchronous='NORMAL')
    try:

        def _insert_then_raise() -> None:
            with transaction(handle.connection):
                handle.connection.execute("INSERT INTO entries VALUES (1, X'01')")
                raise RuntimeError('stop')

        with pytest.raises(RuntimeError, match='stop'):
            _insert_then_raise()

        assert handle.connection.execute('SELECT COUNT(*) FROM entries').fetchone() == (0,)
    finally:
        handle.close()
