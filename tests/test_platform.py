from __future__ import annotations

import os
import select
import socket
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import sg_event_daemon._platform as platform_adapter
from sg_event_daemon._platform import (
    AlreadyLockedError,
    PeerIdentity,
    SecurityError,
    _ensure_supported_platform,
    _UnsupportedPlatformError,
    acquire_lock,
    ensure_secure_state_dir,
    local_socket_path,
    peer_identity,
    reap_child,
    release_flock,
    secure_open_regular_file,
    spawn_child,
    terminate_child,
    try_flock_exclusive,
)
from tests._subprocess_support import subprocess_env

if TYPE_CHECKING:
    from collections.abc import Iterator

_SHORT_TEMP_ROOT = Path(os.sep) / 'tmp'


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _nested_state_dir(root: Path, suffix: str) -> Path:
    root.mkdir(mode=0o700)
    path = root
    for index in range(8):
        path /= f'{suffix}-{index:02d}-abcdefgh'
        path.mkdir(mode=0o700)
    return path


def _read_pipe_notice(fd: int, expected: bytes) -> None:
    deadline = time.monotonic() + 5.0
    received = bytearray()
    while len(received) < len(expected):
        remaining = deadline - time.monotonic()
        assert remaining > 0, f'child did not send {expected!r} within 5 s'
        readable, _writable, _exceptional = select.select((fd,), (), (), remaining)
        assert readable, f'child did not send {expected!r} within 5 s'
        chunk = os.read(fd, len(expected) - len(received))
        assert chunk, f'child closed pipe before sending {expected!r}'
        received.extend(chunk)
    assert received == expected


@pytest.fixture
def short_state_dir() -> Iterator[Path]:
    with tempfile.TemporaryDirectory(
        prefix='sged-',
        dir=os.fspath(_SHORT_TEMP_ROOT),
    ) as directory:
        yield Path(directory)


def test_acquire_lock_creates_owner_only_regular_file(tmp_path: Path) -> None:
    state_dir = ensure_secure_state_dir(tmp_path / 'state')
    path = state_dir / 'daemon.lock'

    with acquire_lock(path):
        assert path.is_file()
        assert _mode(path) == 0o600
        assert path.stat().st_uid == os.geteuid()


def test_acquire_lock_rejects_a_second_owner(tmp_path: Path) -> None:
    state_dir = ensure_secure_state_dir(tmp_path / 'state')
    path = state_dir / 'daemon.lock'

    with acquire_lock(path), pytest.raises(AlreadyLockedError, match='already locked'):
        acquire_lock(path)


def test_acquire_lock_rejects_symlink(tmp_path: Path) -> None:
    state_dir = ensure_secure_state_dir(tmp_path / 'state')
    target = state_dir / 'target'
    target.touch(mode=0o600)
    path = state_dir / 'daemon.lock'
    path.symlink_to(target)

    with pytest.raises(SecurityError, match=r'symlink|regular file'):
        acquire_lock(path)


def test_acquire_lock_rejects_wrong_mode(tmp_path: Path) -> None:
    state_dir = ensure_secure_state_dir(tmp_path / 'state')
    path = state_dir / 'daemon.lock'
    path.touch(mode=0o644)

    with pytest.raises(SecurityError, match='0600'):
        acquire_lock(path)


def test_boot_identity_is_stable_for_the_current_boot() -> None:
    first = platform_adapter.boot_identity()
    second = platform_adapter.boot_identity()

    assert first
    assert second == first


def test_boot_identity_rejects_failed_macos_authoritative_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FailedSysctl:
        def __init__(self) -> None:
            self.argtypes: object = None
            self.restype: object = None

        def __call__(self, *_args: object) -> int:
            return -1

    class _LibC:
        def __init__(self) -> None:
            self.sysctlbyname = _FailedSysctl()

    def find_library(_name: str) -> str:
        return 'libc'

    def load_library(*_args: object, **_options: object) -> _LibC:
        return _LibC()

    monkeypatch.setattr(
        platform_adapter,
        '_ensure_supported_platform',
        lambda: 'macos',
    )
    monkeypatch.setattr(
        platform_adapter.ctypes.util,
        'find_library',
        find_library,
    )
    monkeypatch.setattr(
        platform_adapter.ctypes,
        'CDLL',
        load_library,
    )
    platform_adapter.boot_identity.cache_clear()
    try:
        with pytest.raises(RuntimeError, match=r'kern\.boottime'):
            platform_adapter.boot_identity()
    finally:
        platform_adapter.boot_identity.cache_clear()


def test_closing_a_descriptor_releases_its_flock(tmp_path: Path) -> None:
    path = tmp_path / 'source.db.write-lock'
    first, _ = secure_open_regular_file(path)
    second, _ = secure_open_regular_file(path)
    try:
        assert try_flock_exclusive(first)
        os.close(first)
        first = -1
        assert try_flock_exclusive(second)
    finally:
        if first >= 0:
            os.close(first)
        os.close(second)


def test_ensure_secure_state_dir_creates_owner_only_directory(tmp_path: Path) -> None:
    path = tmp_path / 'state'

    result = ensure_secure_state_dir(path)

    assert result == path
    assert path.is_dir()
    assert _mode(path) == 0o700
    assert path.stat().st_uid == os.geteuid()


def test_ensure_secure_state_dir_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / 'target'
    target.mkdir(mode=0o700)
    path = tmp_path / 'state'
    path.symlink_to(target, target_is_directory=True)

    with pytest.raises(SecurityError, match='symlink'):
        ensure_secure_state_dir(path)


def test_ensure_secure_state_dir_rejects_wrong_mode(tmp_path: Path) -> None:
    path = tmp_path / 'state'
    path.mkdir(mode=0o755)

    with pytest.raises(SecurityError, match='0700'):
        ensure_secure_state_dir(path)


def test_ensure_secure_state_dir_verifies_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / 'state'
    path.mkdir(mode=0o700)
    monkeypatch.setattr(platform_adapter, '_effective_uid', lambda: os.geteuid() + 1)

    with pytest.raises(SecurityError, match='owner'):
        ensure_secure_state_dir(path)


def test_ensure_supported_platform_dispatches_linux_and_macos() -> None:
    assert _ensure_supported_platform('Linux') == 'linux'
    assert _ensure_supported_platform('Darwin') == 'macos'


def test_ensure_supported_platform_rejects_windows() -> None:
    with pytest.raises(_UnsupportedPlatformError, match=r'Windows.*not supported'):
        _ensure_supported_platform('Windows')


def test_file_lock_is_released_after_process_death(tmp_path: Path) -> None:
    state_dir = ensure_secure_state_dir(tmp_path / 'state')
    path = state_dir / 'daemon.lock'
    code = """
import os
import sys
from pathlib import Path
from sg_event_daemon._platform import acquire_lock

lock = acquire_lock(Path(sys.argv[1]))
print('LOCKED', flush=True)
sys.stdin.read()
os._exit(9)
"""
    process = subprocess.Popen(
        [sys.executable, '-c', code, str(path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env=subprocess_env(),
    )
    try:
        assert process.stdout is not None
        _read_pipe_notice(process.stdout.fileno(), b'LOCKED\n')

        with pytest.raises(AlreadyLockedError):
            acquire_lock(path)

        assert process.stdin is not None
        process.stdin.close()
        assert process.wait(timeout=5) == 9
        with acquire_lock(path):
            pass
    finally:
        if process.stdin is not None and not process.stdin.closed:
            process.stdin.close()
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        if process.stdout is not None:
            process.stdout.close()


def test_import_reaches_unsupported_platform_without_fcntl() -> None:
    code = """
import builtins
import platform

original_import = builtins.__import__

def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name == 'fcntl':
        raise ModuleNotFoundError('simulated missing fcntl')
    return original_import(name, globals, locals, fromlist, level)

platform.system = lambda: 'Windows'
builtins.__import__ = blocked_import
from sg_event_daemon._platform import _UnsupportedPlatformError, _ensure_supported_platform

try:
    _ensure_supported_platform()
except _UnsupportedPlatformError as exc:
    print(f'{type(exc).__name__}: {exc}')
else:
    raise AssertionError('Windows platform check did not fail')
"""
    process = subprocess.run(
        [sys.executable, '-c', code],
        check=False,
        capture_output=True,
        text=True,
        env=subprocess_env(),
    )

    assert process.returncode == 0
    assert process.stdout.strip() == (
        '_UnsupportedPlatformError: Windows is not supported by sg-event-daemon'
    )


@pytest.mark.parametrize('use_fallback', [False, True], ids=['ordinary', 'fallback'])
def test_local_socket_path_candidate_matches_existing_paths_without_mutation(
    use_fallback: bool,
    monkeypatch: pytest.MonkeyPatch,
    short_state_dir: Path,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv('XDG_RUNTIME_DIR', raising=False)
    if use_fallback:
        fallback_root = short_state_dir / 'tmp'
        fallback_root.mkdir(mode=0o700)
        monkeypatch.setattr(
            platform_adapter.tempfile,
            'gettempdir',
            lambda: os.fspath(fallback_root),
        )
        state_dir = _nested_state_dir(tmp_path / 'long', 'state') / 'candidate'
    else:
        state_dir = short_state_dir / 'state'

    candidate = platform_adapter.local_socket_path_candidate(
        state_dir,
        'control',
        ('supervisor',),
    )

    assert not state_dir.exists()
    assert not candidate.parent.exists()
    ensure_secure_state_dir(state_dir)
    if candidate.parent != state_dir:
        ensure_secure_state_dir(candidate.parent)
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        stale.bind(os.fspath(candidate))
        candidate.chmod(0o600)
    finally:
        stale.close()

    assert (
        platform_adapter.local_socket_path_candidate(
            state_dir,
            'control',
            ('supervisor',),
        )
        == candidate
    )
    assert candidate.exists()
    assert local_socket_path(state_dir, 'control', ('supervisor',)) == candidate
    assert not candidate.exists()


def test_local_socket_path_cleans_stale_same_uid_socket(short_state_dir: Path) -> None:
    state_dir = ensure_secure_state_dir(short_state_dir)
    path = state_dir / 'control.sock'
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(str(path))
    path.chmod(0o600)
    stale.close()

    result = local_socket_path(state_dir, 'control', ('supervisor',))

    assert result == path
    assert not path.exists()


def test_local_socket_path_does_not_unlink_live_listener(short_state_dir: Path) -> None:
    state_dir = ensure_secure_state_dir(short_state_dir)
    path = state_dir / 'control.sock'
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(path))
    path.chmod(0o600)
    listener.listen()
    try:
        with pytest.raises(SecurityError, match='live listener'):
            local_socket_path(state_dir, 'control', ('supervisor',))
        assert path.exists()
    finally:
        listener.close()
        path.unlink()


def test_local_socket_path_rejects_insecure_existing_path(short_state_dir: Path) -> None:
    state_dir = ensure_secure_state_dir(short_state_dir)
    path = state_dir / 'control.sock'
    path.write_text('not a socket', encoding='utf-8')

    with pytest.raises(SecurityError, match='socket'):
        local_socket_path(state_dir, 'control', ('supervisor',))


def test_local_socket_path_rejects_invalid_kind_without_creating_state_dir(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / 'state'

    with pytest.raises(SecurityError, match='invalid socket kind'):
        local_socket_path(state_dir, '../control', ('supervisor',))

    assert not state_dir.exists()


def test_local_socket_path_rejects_wrong_socket_mode(short_state_dir: Path) -> None:
    state_dir = ensure_secure_state_dir(short_state_dir)
    path = state_dir / 'control.sock'
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(str(path))
    path.chmod(0o666)
    stale.close()

    with pytest.raises(SecurityError, match='0600'):
        local_socket_path(state_dir, 'control', ('supervisor',))
    assert path.exists()
    path.unlink()


@pytest.mark.parametrize('kind', ['control', 'events'])
def test_local_socket_path_uses_collision_resistant_short_fallback(
    kind: str,
    monkeypatch: pytest.MonkeyPatch,
    short_state_dir: Path,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv('XDG_RUNTIME_DIR', raising=False)
    monkeypatch.setattr(
        platform_adapter.tempfile,
        'gettempdir',
        lambda: os.fspath(short_state_dir),
    )
    first_state_dir = _nested_state_dir(tmp_path / 'first', 'state')
    second_state_dir = _nested_state_dir(tmp_path / 'second', 'state')

    first = local_socket_path(first_state_dir, kind, ('site', 'source'))
    second = local_socket_path(second_state_dir, kind, ('site', 'source'))

    assert first != second
    first_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    second_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        first_socket.bind(os.fspath(first))
        second_socket.bind(os.fspath(second))
    finally:
        first_socket.close()
        second_socket.close()
        first.unlink(missing_ok=True)
        second.unlink(missing_ok=True)
    assert first.parent == short_state_dir / f'sg-event-daemon-{os.geteuid()}'
    assert _mode(first.parent) == 0o700


def test_local_socket_path_uses_state_directory_when_it_fits(short_state_dir: Path) -> None:
    state_dir = ensure_secure_state_dir(short_state_dir)

    assert local_socket_path(state_dir, 'control', ('supervisor',)) == (state_dir / 'control.sock')


def test_peer_identity_uses_kernel_credentials() -> None:
    first, second = socket.socketpair()
    try:
        identity = peer_identity(first)
    finally:
        first.close()
        second.close()

    assert identity == PeerIdentity(uid=os.geteuid(), gid=os.getegid(), pid=os.getpid())


def test_pending_socket_bytes_preserves_the_receive_queue() -> None:
    sender, receiver = socket.socketpair()
    with sender, receiver:
        sender.sendall(b'first')
        assert platform_adapter.pending_socket_bytes(receiver) == 5
        sender.sendall(b'later')
        assert receiver.recv(5) == b'first'
        assert platform_adapter.pending_socket_bytes(receiver) == 5
        assert receiver.recv(5) == b'later'
        assert platform_adapter.pending_socket_bytes(receiver) == 0
        sender.close()
        assert platform_adapter.pending_socket_bytes(receiver) == 0
        assert receiver.recv(1) == b''


def test_process_helpers_spawn_with_an_exact_descriptor_allowlist() -> None:
    control_read, control_write = os.pipe()
    sentinel_read, sentinel_write = os.pipe()
    inheritable = True
    os.set_inheritable(sentinel_write, inheritable)
    code = """
import os
import sys

control_fd = int(sys.argv[1])
sentinel_fd = int(sys.argv[2])
os.write(control_fd, b'R')
try:
    os.fstat(sentinel_fd)
except OSError:
    raise SystemExit(0)
raise SystemExit(9)
"""
    process = None
    try:
        process = spawn_child(
            dict.fromkeys(
                (sys.executable, '-c', code, str(control_write), str(sentinel_write))
            ).keys(),
            pass_fds=frozenset((control_write,)),
        )
        os.close(control_write)
        control_write = -1
        os.close(sentinel_write)
        sentinel_write = -1

        _read_pipe_notice(control_read, b'R')
        assert reap_child(process, timeout=5.0) == 0
        assert os.read(sentinel_read, 1) == b''
    finally:
        os.close(control_read)
        os.close(sentinel_read)
        if control_write >= 0:
            os.close(control_write)
        if sentinel_write >= 0:
            os.close(sentinel_write)
        if process is not None and reap_child(process) is None:
            terminate_child(process, force=True)
            reap_child(process, timeout=5.0)


def test_process_helpers_terminate_and_reap_a_running_child() -> None:
    process = spawn_child(
        [
            sys.executable,
            '-c',
            'import time; time.sleep(30)',
        ]
    )
    try:
        assert reap_child(process) is None

        terminate_child(process)

        return_code = reap_child(process, timeout=5.0)
        assert return_code is not None
        assert return_code != 0
    finally:
        if reap_child(process) is None:
            terminate_child(process, force=True)
            reap_child(process, timeout=5.0)


def test_secure_open_without_create_rejects_missing_file(tmp_path: Path) -> None:
    path = tmp_path / 'missing.db'

    with pytest.raises(FileNotFoundError):
        secure_open_regular_file(path, create=False)

    assert not path.exists()


@pytest.mark.parametrize('runtime_source', ['xdg', 'temporary'])
def test_socket_fallback_uses_only_a_validated_private_root(
    runtime_source: str,
    monkeypatch: pytest.MonkeyPatch,
    short_state_dir: Path,
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / ('long-state-' * 20)
    monkeypatch.setenv('XDG_RUNTIME_DIR', os.fspath(short_state_dir / 'missing'))
    monkeypatch.setattr(
        platform_adapter.tempfile, 'gettempdir', lambda: os.fspath(_SHORT_TEMP_ROOT)
    )
    if runtime_source == 'xdg':
        monkeypatch.setenv('XDG_RUNTIME_DIR', os.fspath(short_state_dir))
        temporary_root = short_state_dir / 'temporary'
        temporary_root.mkdir(mode=0o700)
        monkeypatch.setattr(
            platform_adapter.tempfile, 'gettempdir', lambda: os.fspath(temporary_root)
        )
    else:
        monkeypatch.setattr(
            platform_adapter.tempfile, 'gettempdir', lambda: os.fspath(short_state_dir)
        )

    candidate = platform_adapter.local_socket_path_candidate(state_dir, 'control', ())

    assert candidate.parent.parent == short_state_dir
    assert not candidate.parent.exists()
    assert not state_dir.exists()


@pytest.mark.parametrize('usable_xdg', [False, True])
def test_socket_fallback_handles_unavailable_temporary_directory(
    usable_xdg: bool,
    monkeypatch: pytest.MonkeyPatch,
    short_state_dir: Path,
    tmp_path: Path,
) -> None:
    root = short_state_dir if usable_xdg else short_state_dir / 'missing'
    monkeypatch.setenv('XDG_RUNTIME_DIR', os.fspath(root))
    temporary_lookups = 0

    def unavailable_temporary_directory() -> str:
        nonlocal temporary_lookups
        temporary_lookups += 1
        raise FileNotFoundError('no usable temporary directory')

    monkeypatch.setattr(platform_adapter.tempfile, 'gettempdir', unavailable_temporary_directory)
    state_dir = tmp_path / ('long-state-' * 20)
    before = set(short_state_dir.rglob('*'))

    if usable_xdg:
        candidate = platform_adapter.local_socket_path_candidate(state_dir, 'control', ())
        assert candidate.parent.parent == root
        assert temporary_lookups == 0
    else:
        with pytest.raises(SecurityError, match=r'shorter state.*XDG_RUNTIME_DIR'):
            platform_adapter.local_socket_path_candidate(state_dir, 'control', ())
        assert temporary_lookups == 1

    assert set(short_state_dir.rglob('*')) == before
    assert not state_dir.exists()


@pytest.mark.parametrize('unsafe_root', ['shared', 'symlink', 'foreign', 'missing', 'long'])
def test_socket_fallback_rejects_unsafe_roots_without_mutation(
    unsafe_root: str,
    monkeypatch: pytest.MonkeyPatch,
    short_state_dir: Path,
    tmp_path: Path,
) -> None:
    root = short_state_dir / 'runtime'
    if unsafe_root == 'symlink':
        root.symlink_to(short_state_dir, target_is_directory=True)
    elif unsafe_root == 'long':
        root = _nested_state_dir(tmp_path / 'runtime', 'long')
    elif unsafe_root != 'missing':
        root.mkdir(mode=0o755 if unsafe_root == 'shared' else 0o700)
    if unsafe_root == 'foreign':
        monkeypatch.setattr(platform_adapter, '_effective_uid', lambda: os.geteuid() + 1)
    monkeypatch.setenv('XDG_RUNTIME_DIR', os.fspath(root))
    monkeypatch.setattr(platform_adapter.tempfile, 'gettempdir', lambda: os.fspath(root))
    state_dir = tmp_path / ('long-state-' * 20)
    before = set(short_state_dir.rglob('*'))

    with pytest.raises(SecurityError, match=r'shorter state.*XDG_RUNTIME_DIR'):
        platform_adapter.local_socket_path_candidate(state_dir, 'control', ())

    assert set(short_state_dir.rglob('*')) == before
    assert not state_dir.exists()


@pytest.mark.parametrize(('platform_name', 'limit'), [('Linux', 107), ('Darwin', 103)])
def test_socket_path_limits_count_encoded_bytes(
    platform_name: str,
    limit: int,
    monkeypatch: pytest.MonkeyPatch,
    short_state_dir: Path,
) -> None:
    monkeypatch.setattr(platform_adapter.platform, 'system', lambda: platform_name)
    monkeypatch.setenv('XDG_RUNTIME_DIR', os.fspath(short_state_dir))
    # The multibyte component keeps character count below the byte limit.
    state_dir = Path('/') / ('\N{LATIN SMALL LETTER E WITH ACUTE}' * 20 + 'a' * (limit - 54))
    ordinary = state_dir / 'control.sock'
    assert len(os.fsencode(ordinary)) == limit
    assert platform_adapter.local_socket_path_candidate(state_dir, 'control', ()) == ordinary
    longer = state_dir.with_name(state_dir.name + 'a')
    assert platform_adapter.local_socket_path_candidate(longer, 'control', ()).parent.parent == (
        short_state_dir
    )


def test_try_flock_exclusive_conflicts_across_open_file_descriptions(tmp_path: Path) -> None:
    path = tmp_path / 'source.db.write-lock'
    first, _ = secure_open_regular_file(path)
    second, _ = secure_open_regular_file(path)
    try:
        assert try_flock_exclusive(first)
        assert not try_flock_exclusive(second)
        release_flock(first)
        assert try_flock_exclusive(second)
    finally:
        os.close(first)
        os.close(second)


def test_validate_secure_directory_does_not_create_missing_directory(tmp_path: Path) -> None:
    path = tmp_path / 'missing'

    with pytest.raises(FileNotFoundError):
        platform_adapter.validate_secure_directory(path)

    assert not path.exists()


def test_validate_secure_directory_rejects_foreign_directory_owner(
    monkeypatch: pytest.MonkeyPatch,
    short_state_dir: Path,
) -> None:
    monkeypatch.setattr(platform_adapter, '_effective_uid', lambda: os.geteuid() + 1)
    before = short_state_dir.lstat()

    with pytest.raises(SecurityError, match='state directory has the wrong owner'):
        platform_adapter.validate_secure_directory(short_state_dir)

    assert short_state_dir.lstat() == before
