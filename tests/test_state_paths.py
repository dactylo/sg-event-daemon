from pathlib import Path

from sg_event_daemon._config import Config, DaemonConfig
from sg_event_daemon._state_paths import (
    group_lock_path,
    site_groups_directory,
    site_sources_directory,
    source_database_path,
    source_lock_path,
    source_socket_kind,
    source_write_lock_path,
)


def test_source_socket_kind_matches_collision_normalization() -> None:
    assert source_socket_kind('source_a') == 'source-a-events'
    assert source_socket_kind('source-a') == 'source-a-events'


def test_state_database_paths_preserve_layout_without_creating_directories(
    tmp_path: Path,
) -> None:
    config = Config(
        daemon=DaemonConfig(state_dir=tmp_path / 'state', sg_api_provider='shotgun-api3'),
        sites={},
    )

    assert source_database_path(config, 'site-a', 'source-a') == (
        tmp_path / 'state' / 'sites' / 'site-a' / 'sources' / 'source-a.db'
    )
    assert site_sources_directory(config, 'site-a') == (
        tmp_path / 'state' / 'sites' / 'site-a' / 'sources'
    )
    assert site_groups_directory(config, 'site-a') == (
        tmp_path / 'state' / 'sites' / 'site-a' / 'groups'
    )
    assert not (tmp_path / 'state').exists()


def test_worker_lock_paths_preserve_layout_without_creating_directories(
    tmp_path: Path,
) -> None:
    config = Config(
        daemon=DaemonConfig(state_dir=tmp_path / 'state', sg_api_provider='shotgun-api3'),
        sites={},
    )

    assert source_lock_path(source_database_path(config, 'site-a', 'source-a')) == (
        tmp_path / 'state' / 'sites' / 'site-a' / 'sources' / 'source-a.db.lock'
    )
    assert group_lock_path(config, 'site-a', 'group-a') == (
        tmp_path / 'state' / 'sites' / 'site-a' / 'groups' / 'group-a.lock'
    )
    assert source_write_lock_path(config, 'site-a', 'source-a') == (
        tmp_path / 'state' / 'sites' / 'site-a' / 'sources' / 'source-a.db.write-lock'
    )
    assert not (tmp_path / 'state').exists()
