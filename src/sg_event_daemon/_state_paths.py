"""Canonical paths for source and group state databases."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._config import Config


def site_groups_directory(config: Config, site: str) -> Path:
    """Return a site's group lock directory without creating it."""
    return config.daemon.state_dir / 'sites' / site / 'groups'


def group_lock_path(config: Config, site: str, group: str) -> Path:
    """Return the configured group lock path without creating directories.

    Args:
        config: Resolved daemon configuration.
        site: Configured site name.
        group: Configured group name.

    Returns:
        Path to the group's lock file.
    """
    return site_groups_directory(config, site) / f'{group}.lock'


def site_sources_directory(config: Config, site: str) -> Path:
    """Return a site's source state directory without creating it."""
    return config.daemon.state_dir / 'sites' / site / 'sources'


def source_database_path(config: Config, site: str, source: str) -> Path:
    """Return the configured source database path without creating directories.

    Args:
        config: Resolved daemon configuration.
        site: Configured site name.
        source: Configured source name.

    Returns:
        Path to the source's SQLite database.
    """
    return site_sources_directory(config, site) / f'{source}.db'


def source_lock_path(database_path: Path) -> Path:
    """Return the owner lock path for a source database without creating it."""
    return Path(f'{database_path}.lock')


def source_socket_kind(source: str) -> str:
    """Return the normalized local event socket kind for a source name."""
    return f'{source.replace("_", "-")}-events'


def source_write_lock_path(config: Config, site: str, source: str) -> Path:
    """Return the configured source write-lock path without creating directories.

    Args:
        config: Resolved daemon configuration.
        site: Configured site name.
        source: Configured source name.

    Returns:
        Path to the source's write-lock file.
    """
    return site_sources_directory(config, site) / f'{source}.db.write-lock'
