import os
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from _pytest.capture import CaptureManager

from tests._runtime_diagnostics import install_runtime_diagnostics

pytest_plugins = ('pytester', 'tests._live_mode')


@pytest.fixture(scope='session')
def _private_runtime_directory() -> Iterator[str]:
    with tempfile.TemporaryDirectory(prefix='sged-', dir=Path(os.sep) / 'tmp') as directory:
        yield directory


@pytest.fixture(autouse=True)
def _private_runtime_root(
    monkeypatch: pytest.MonkeyPatch,
    _private_runtime_directory: str,
) -> None:
    monkeypatch.setenv('XDG_RUNTIME_DIR', _private_runtime_directory)


@pytest.fixture(scope='session')
def _runtime_stderr_fd(request: pytest.FixtureRequest) -> Iterator[int]:
    capture = request.config.pluginmanager.getplugin('capturemanager')
    assert isinstance(capture, CaptureManager)
    with capture.global_and_fixture_disabled():
        descriptor = os.dup(2)
    try:
        yield descriptor
    finally:
        os.close(descriptor)


@pytest.fixture(autouse=True)
def _runtime_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
    _runtime_stderr_fd: int,
) -> None:
    destination = tmp_path_factory.getbasetemp() / f'runtime-diagnostics-{os.getpid()}.log'
    install_runtime_diagnostics(monkeypatch, destination, _runtime_stderr_fd)
