import logging
import os
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from _pytest.capture import CaptureManager

import sg_event_daemon._logging as logging_module
from tests._runtime_diagnostics import install_runtime_diagnostics
from tests._stall_policy import StallDetectors, activate_stall_policy

pytest_plugins = ('pytester', 'tests._live_mode')


@pytest.fixture(autouse=True)
def _restore_daemon_logger() -> Iterator[None]:
    logger = logging.getLogger('sg_event_daemon')
    old_handlers = list(logger.handlers)
    old_level = logger.level
    old_propagate = logger.propagate
    with logging_module._REDACTION_LOCK:
        old_values = set(logging_module._REDACTION_VALUES)
        old_ordered_values = logging_module._ordered_redaction_values
    try:
        yield
    finally:
        for handler in logger.handlers:
            if handler not in old_handlers:
                handler.close()
        logger.handlers[:] = old_handlers
        logger.setLevel(old_level)
        logger.propagate = old_propagate
        with logging_module._REDACTION_LOCK:
            logging_module._REDACTION_VALUES.clear()
            logging_module._REDACTION_VALUES.update(old_values)
            logging_module._ordered_redaction_values = old_ordered_values


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


@pytest.fixture(autouse=True)
def stall_detectors(monkeypatch: pytest.MonkeyPatch) -> StallDetectors:
    activate_stall_policy(monkeypatch)
    return StallDetectors(monkeypatch)
