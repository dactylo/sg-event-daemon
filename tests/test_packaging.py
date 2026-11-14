import os
import shutil
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path

from tests.cli_helpers import process_transcript, run_cli, run_setup

_ROOT = Path(__file__).resolve().parents[1]


def _build(project: Path, target: str) -> Path:
    result = run_setup(
        [sys.executable, '-m', 'hatchling', 'build', '-t', target], cwd=project, check=False
    )
    assert result.returncode == 0, process_transcript(result)
    suffix = '*.whl' if target == 'wheel' else '*.tar.gz'
    return next((project / 'dist').glob(suffix))


def _copy_project(destination: Path) -> None:
    destination.mkdir()
    for name in ('pyproject.toml', 'README.md', 'LICENSE'):
        shutil.copy2(_ROOT / name, destination / name)
    shutil.copytree(
        _ROOT / 'src',
        destination / 'src',
        ignore=shutil.ignore_patterns('__pycache__', '*.pyc'),
    )


def test_installed_wheel_reports_its_version(tmp_path: Path) -> None:
    project = tmp_path / 'project'
    _copy_project(project)
    expected = tomllib.loads((project / 'pyproject.toml').read_text(encoding='utf-8'))['project'][
        'version'
    ]
    wheel = _build(project, 'wheel')
    installed = tmp_path / 'installed'
    with zipfile.ZipFile(wheel) as archive:
        for member in archive.namelist():
            archive.extract(member, installed)

    result = run_cli(
        ['--version'],
        expect=0,
        cwd=tmp_path,
        env={**os.environ, 'PYTHONPATH': str(installed)},
    )
    assert result.stdout == f'sg-event-daemon {expected}\n', process_transcript(result)


def test_sdist_excludes_repository_support_files(tmp_path: Path) -> None:
    project = tmp_path / 'project'
    _copy_project(project)
    for name in ('CHANGELOG.md', 'Makefile'):
        shutil.copy2(_ROOT / name, project / name)
    for directory, name in (
        ('benchmarks', 'storage_load.py'),
        ('docs', 'development.md'),
        ('tests', 'test_packaging.py'),
    ):
        (project / directory).mkdir()
        shutil.copy2(_ROOT / directory / name, project / directory / name)

    with tarfile.open(_build(project, 'sdist'), 'r:gz') as archive:
        names = {name.split('/', 1)[1] for name in archive.getnames() if '/' in name}

    assert {name.split('/', 1)[0] for name in names} == {
        'LICENSE',
        'PKG-INFO',
        'README.md',
        'pyproject.toml',
        'src',
    }
    assert all(
        name.startswith('src/sg_event_daemon/') for name in names if name.startswith('src/')
    )
