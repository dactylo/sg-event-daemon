from __future__ import annotations

import sqlite3
from contextlib import ExitStack, closing
from typing import TYPE_CHECKING

import pytest

from sg_event_daemon._group_state import GroupOpenOptions, GroupState
from sg_event_daemon._ipc import DeliveryGroupAssignment, GroupBootstrapObservation
from sg_event_daemon._platform import AlreadyLockedError, ensure_secure_state_dir
from sg_event_daemon._source import CatchupReader
from sg_event_daemon._source_schema import SOURCE_SCHEMA, source_identity
from sg_event_daemon._source_state import SourceState
from sg_event_daemon._sqlite import DatabaseError, open_database
from sg_event_daemon._supervisor import Supervisor
from sg_event_daemon._worker import _LaneComponent, _LaneConfiguration, build_worker_specs
from tests._storage_support import write_context_for
from tests.test_supervisor import _source_only_config

if TYPE_CHECKING:
    from pathlib import Path


def _options(group: str, path: Path) -> GroupOpenOptions:
    return {
        'writes': write_context_for(path),
        'site': 'test',
        'source': 'admin',
        'site_server_url': 'https://example.invalid',
        'group': group,
        'group_generation': 1,
        'ordering': 'eager',
        'stream_fingerprint': 'fingerprint',
        'source_generation': 'generation',
        'verified_incarnation': 'incarnation',
        'start_mode': 'initialize',
        'initial_cursor': 0,
        'initial_delivery_floor': 0,
        'initial_last_delivered_id': 0,
    }


def _paths(tmp_path: Path) -> tuple[Path, Path]:
    sites = ensure_secure_state_dir(tmp_path / 'sites')
    site = ensure_secure_state_dir(sites / 'test')
    sources = ensure_secure_state_dir(site / 'sources')
    groups = ensure_secure_state_dir(site / 'groups')
    return sources / 'admin.db', groups


def _source(path: Path, *, create: bool = False) -> SourceState:
    state = SourceState.open(
        path,
        writes=write_context_for(path),
        create=create,
        site='test',
        source='admin',
        stream_fingerprint='fingerprint',
        source_generation='generation',
    )
    state.initialize(0, incarnation='incarnation')
    return state


def test_daemon_creates_exactly_the_part_1_schema(tmp_path: Path) -> None:
    path, _groups = _paths(tmp_path)
    with SourceState.open(
        path,
        writes=write_context_for(path),
        create=True,
        site='test',
        source='admin',
        stream_fingerprint='fingerprint',
        source_generation='generation',
    ):
        pass
    with (
        open_database(
            tmp_path / 'canonical.db',
            source_identity('test', 'admin'),
            SOURCE_SCHEMA,
            synchronous='NORMAL',
        ) as canonical,
        closing(sqlite3.connect(path)) as actual,
    ):
        statement = (
            'SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name, tbl_name'
        )
        assert (
            actual.execute(statement).fetchall()
            == canonical.connection.execute(statement).fetchall()
        )


@pytest.mark.parametrize('empty', [False, True])
def test_group_open_requires_an_existing_initialized_source_file(
    tmp_path: Path, *, empty: bool
) -> None:
    path, groups = _paths(tmp_path)
    if empty:
        path.touch(mode=0o600)
    with pytest.raises(DatabaseError):
        GroupState.open(path, groups / 'one.lock', **_options('one', path))
    assert path.exists() is empty
    if empty:
        assert path.read_bytes() == b''
    assert not list(path.parent.glob('*-wal'))


def test_group_owner_lock_excludes_a_second_owner(tmp_path: Path) -> None:
    path, groups = _paths(tmp_path)
    with (
        _source(path, create=True),
        GroupState.open(path, groups / 'one.lock', **_options('one', path)),
    ):
        with pytest.raises(AlreadyLockedError):
            GroupState.open(path, groups / 'one.lock', **_options('one', path))
        with GroupState.open(path, groups / 'two.lock', **_options('two', path)) as second:
            assert second.snapshot().group == 'two'


def test_groups_share_their_source_file(tmp_path: Path) -> None:
    path, groups = _paths(tmp_path)
    with ExitStack() as stack:
        stack.enter_context(_source(path, create=True))
        for group in ('one', 'two'):
            state = stack.enter_context(
                GroupState.open(path, groups / f'{group}.lock', **_options(group, path))
            )
            assert state.snapshot().group == group
        with closing(sqlite3.connect(path)) as connection:
            assert connection.execute(
                'SELECT group_name FROM groups ORDER BY group_name'
            ).fetchall() == [('one',), ('two',)]
    assert list(path.parent.glob('*.db')) == [path]
    assert not list(groups.glob('*.db'))
    assert sorted(item.name for item in groups.iterdir()) == ['one.lock', 'two.lock']


def test_only_the_source_component_creates_the_source_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, config = _source_only_config(tmp_path, monkeypatch)
    ensure_secure_state_dir(config.daemon.state_dir)
    path, _groups = _paths(config.daemon.state_dir)
    with pytest.raises(DatabaseError):
        SourceState.open(
            path,
            writes=write_context_for(path),
            site='test',
            source='admin',
            stream_fingerprint='fingerprint',
            source_generation='generation',
        )
    assert not path.exists()
    with Supervisor(config=config, config_path=config_path) as supervisor, ExitStack() as stack:
        assert supervisor._open_reconciliation_sources(stack, {('test', 'admin')}) == {}
        with pytest.raises(sqlite3.OperationalError):
            supervisor._orphan_group_metadata(site='test', group='remote')
    site = config.sites['test']
    group = site.groups['remote']
    source = site.sources[group.source]
    lane = object.__new__(_LaneComponent)
    vars(lane).update(
        _config=config,
        _spec=next(spec for spec in build_worker_specs(site) if 'remote' in spec.groups),
        _group='remote',
        _write_context=lambda: write_context_for(path),
    )
    context = _LaneConfiguration(
        _assignment=DeliveryGroupAssignment('admin', 'remote', 1, 'resume'),
        _credential=site.credentials[source.credentials],
        _group=group,
        _holdback_secs=0.0,
        _site=site,
        _source_config=source,
    )
    observation = GroupBootstrapObservation(
        'admin', 'remote', 1, 'generation', 'incarnation', 0, 0.0
    )
    with pytest.raises(DatabaseError):
        lane._open_group_state(context, observation, object.__new__(CatchupReader))
    assert not path.exists()
    assert not list(_groups.glob('*.db'))
    with _source(path, create=True):
        assert path.is_file()
