"""Focused Linux and macOS security adapters."""

from __future__ import annotations

import array
import base64
import ctypes
import ctypes.util
import errno
import hashlib
import importlib
import json
import os
import platform
import re
import socket
import stat
import struct
import subprocess
import tempfile
from contextlib import suppress
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterable, Mapping
    from types import TracebackType
    from typing import Self

ChildProcess: TypeAlias = subprocess.Popen[bytes]

_DIRECTORY_MODE = 0o700
FILE_MODE = 0o600
_KIND_PATTERN = re.compile(r'[a-z][a-z0-9-]*\Z')
_LINUX_SOCKET_PATH_LIMIT = 107
_LOCAL_PEERPID = 2
_MACOS_SOCKET_PATH_LIMIT = 103
_SO_LOCAL = 0


class AlreadyLockedError(RuntimeError):
    """Report an instance lock already held by another process."""


class FileLock:
    """Own one advisory process lock descriptor.

    The lock remains held until `close()` or context exit releases it and
    closes the descriptor. Repeated calls to `close()` are harmless.
    """

    def __init__(self, fd: int, path: Path) -> None:
        """Store the locked descriptor and its diagnostic path."""
        self._fd = fd
        self._path = path

    def __enter__(self) -> Self:
        """Return this held lock.

        Returns:
            This lock while its descriptor remains open.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release the lock when leaving a context."""
        self.close()

    def close(self) -> None:
        """Release the advisory lock and close its descriptor."""
        if self._fd < 0:
            return
        fd = self._fd
        self._fd = -1
        try:
            _flock(fd, 'LOCK_UN')
        finally:
            os.close(fd)


@dataclass(frozen=True)
class PeerIdentity:
    """Contain credentials obtained from the local socket kernel."""

    uid: int
    gid: int
    pid: int


class SecurityError(RuntimeError):
    """Report an unsafe local filesystem or socket object."""


class _UnsupportedPlatformError(RuntimeError):
    """Report an operating system unsupported by this release."""


def _effective_uid() -> int:
    """Return the effective local user id."""
    return os.geteuid()


def _ensure_supported_platform(platform_name: str | None = None) -> str:
    """Validate and normalize the selected operating system."""
    selected = platform.system() if platform_name is None else platform_name
    if selected == 'Linux':
        return 'linux'
    if selected == 'Darwin':
        return 'macos'
    raise _UnsupportedPlatformError(f'{selected} is not supported by sg-event-daemon')


def _flock(fd: int, *flag_names: str) -> None:
    """Apply one supported-platform advisory lock operation."""
    module = importlib.import_module('fcntl')
    flock = cast('Callable[[int, int], None]', module.flock)
    operation = sum(cast('int', getattr(module, name)) for name in flag_names)
    flock(fd, operation)


def _peer_identity_linux(sock: socket.socket) -> PeerIdentity:
    """Read Linux peer credentials from `sock`."""
    option = getattr(socket, 'SO_PEERCRED', 17)
    size = struct.calcsize('3i')
    try:
        credentials = sock.getsockopt(socket.SOL_SOCKET, option, size)
    except OSError as exc:
        raise SecurityError(f'cannot read Linux peer credentials: {exc}') from exc
    pid, uid, gid = struct.unpack('3i', credentials)
    return PeerIdentity(uid=uid, gid=gid, pid=pid)


def _peer_identity_macos(sock: socket.socket) -> PeerIdentity:
    """Read macOS peer credentials from `sock`."""
    library_name = ctypes.util.find_library('c')
    libc = ctypes.CDLL(library_name or None, use_errno=True)
    uid = ctypes.c_uint()
    gid = ctypes.c_uint()
    getpeereid = libc.getpeereid
    getpeereid.argtypes = [
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_uint),
        ctypes.POINTER(ctypes.c_uint),
    ]
    getpeereid.restype = ctypes.c_int
    if getpeereid(sock.fileno(), ctypes.byref(uid), ctypes.byref(gid)) != 0:
        error_number = ctypes.get_errno()
        raise SecurityError(
            f'cannot read macOS peer user credentials: {os.strerror(error_number)}'
        )
    try:
        pid_bytes = sock.getsockopt(_SO_LOCAL, _LOCAL_PEERPID, struct.calcsize('i'))
    except OSError as exc:
        raise SecurityError(f'cannot read macOS peer pid: {exc}') from exc
    (pid,) = struct.unpack('i', pid_bytes)
    return PeerIdentity(uid=uid.value, gid=gid.value, pid=pid)


def _socket_digest(state_dir: Path, kind: str, identity: Iterable[str]) -> str:
    """Return a compact collision-resistant socket identity digest."""
    structured = json.dumps(
        [os.fspath(state_dir.resolve(strict=False)), kind, list(identity)],
        ensure_ascii=True,
        separators=(',', ':'),
    ).encode()
    digest = hashlib.sha256(structured).digest()[:16]
    return base64.urlsafe_b64encode(digest).decode().rstrip('=')


def _socket_is_live(path: Path) -> bool:
    """Return whether a Unix socket path accepts a connection."""
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.connect(os.fspath(path))
    except OSError as exc:
        if exc.errno in {errno.ECONNREFUSED, errno.ENOENT, errno.ECONNRESET}:
            return False
        raise SecurityError(f'cannot verify socket listener {path}: {exc}') from exc
    finally:
        probe.close()
    return True


def _socket_path_name(kind: str) -> str:
    """Return the ordinary filename for one socket kind."""
    return f'{kind}.sock'


def _unix_socket_path_limit(platform_name: str | None = None) -> int:
    """Return the usable encoded Unix socket path limit."""
    selected = _ensure_supported_platform(platform_name)
    if selected == 'linux':
        return _LINUX_SOCKET_PATH_LIMIT
    return _MACOS_SOCKET_PATH_LIMIT


def _verify_directory(path: Path, metadata: os.stat_result) -> None:
    """Verify one daemon-owned mode-0700 directory."""
    if not stat.S_ISDIR(metadata.st_mode):
        raise SecurityError(f'state path is not a directory: {path}')
    if metadata.st_uid != _effective_uid():
        raise SecurityError(f'state directory has the wrong owner: {path}')
    if stat.S_IMODE(metadata.st_mode) != _DIRECTORY_MODE:
        raise SecurityError(f'state directory must use mode 0700: {path}')


def _open_and_verify_directory(
    path: Path,
    parent_fd: int,
    *,
    state_directory: bool,
) -> None:
    """Open and verify a directory relative to its validated parent."""
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
    if not state_directory or hasattr(os, 'O_NOFOLLOW'):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path.name, flags, dir_fd=parent_fd)
    except FileNotFoundError as exc:
        if not state_directory:
            raise
        raise SecurityError(f'cannot securely open state directory {path}: {exc}') from exc
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            description = (
                'state directory is a symlink'
                if state_directory
                else 'directory is a symlink or not a directory'
            )
            raise SecurityError(f'{description}: {path}') from exc
        description = 'state directory' if state_directory else 'directory'
        raise SecurityError(f'cannot securely open {description} {path}: {exc}') from exc
    try:
        _verify_directory(path, os.fstat(fd))
    finally:
        os.close(fd)


def _verify_parent_directory(path: Path, metadata: os.stat_result) -> None:
    """Verify one parent directory cannot be replaced by an untrusted user."""
    if not stat.S_ISDIR(metadata.st_mode):
        raise SecurityError(f'parent path is not a directory: {path}')
    if metadata.st_uid not in {0, _effective_uid()}:
        raise SecurityError(f'parent directory has an unsafe owner: {path}')
    mode = stat.S_IMODE(metadata.st_mode)
    if mode & 0o022 and not metadata.st_mode & stat.S_ISVTX:
        raise SecurityError(f'parent directory is writable by other users: {path}')


def _open_parent_directory(path: Path) -> int:
    """Open and validate the resolved parent of `path`."""
    parent = path.absolute().parent.resolve(strict=True)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC
    if hasattr(os, 'O_NOFOLLOW'):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(parent, flags)
    except OSError as exc:
        raise SecurityError(f'cannot securely open parent directory {parent}: {exc}') from exc
    try:
        metadata = os.fstat(fd)
        _verify_parent_directory(parent, metadata)
    except BaseException:
        os.close(fd)
        raise
    return fd


def _verify_regular_file(path: Path, metadata: os.stat_result) -> None:
    """Verify one daemon-owned mode-0600 regular file."""
    if not stat.S_ISREG(metadata.st_mode):
        raise SecurityError(f'state file is not a regular file: {path}')
    if metadata.st_uid != _effective_uid():
        raise SecurityError(f'state file has the wrong owner: {path}')
    if stat.S_IMODE(metadata.st_mode) != FILE_MODE:
        raise SecurityError(f'state file must use mode 0600: {path}')


@cache
def boot_identity() -> str:
    """Return one stable identity for the running operating-system boot.

    Returns:
        A nonempty platform-qualified boot identity.

    Raises:
        RuntimeError: If the supported platform cannot expose boot identity.
    """
    selected = _ensure_supported_platform()
    if selected == 'linux':
        path = Path('/proc/sys/kernel/random/boot_id')
        try:
            value = path.read_text(encoding='ascii').strip()
        except OSError as exc:
            raise RuntimeError(f'cannot read Linux boot identity: {path}') from exc
        if not value:
            raise RuntimeError(f'Linux boot identity is empty: {path}')
        return f'linux:{value}'

    class _Timeval(ctypes.Structure):
        """Match the macOS `timeval` boot-time result."""

        _fields_ = [('seconds', ctypes.c_long), ('microseconds', ctypes.c_int)]

    library_name = ctypes.util.find_library('c')
    libc = ctypes.CDLL(library_name or None, use_errno=True)
    boot_time = _Timeval()
    size = ctypes.c_size_t(ctypes.sizeof(boot_time))
    sysctlbyname = libc.sysctlbyname
    sysctlbyname.argtypes = [
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_void_p,
        ctypes.c_size_t,
    ]
    sysctlbyname.restype = ctypes.c_int
    if (
        sysctlbyname(
            b'kern.boottime',
            ctypes.byref(boot_time),
            ctypes.byref(size),
            None,
            0,
        )
        != 0
    ):
        error_number = ctypes.get_errno()
        raise RuntimeError('cannot read macOS boot identity: kern.boottime') from OSError(
            error_number, os.strerror(error_number)
        )
    return f'macos:{boot_time.seconds}:{boot_time.microseconds}'


def ensure_secure_state_dir(path: Path) -> Path:
    """Create or validate the daemon's owner-only state directory.

    Args:
        path: State directory to create or validate.

    Returns:
        The input path after secure validation.

    Raises:
        SecurityError: If the path is linked, misowned, or not mode 0700;
            or its parent cannot be opened securely, is owned by neither root
            nor the daemon user, or is writable by others without the sticky
            bit.
        RuntimeError: If the current platform is unsupported.
    """
    _ensure_supported_platform()
    parent_fd = _open_parent_directory(path)
    try:
        with suppress(FileExistsError):
            os.mkdir(path.name, _DIRECTORY_MODE, dir_fd=parent_fd)
        _open_and_verify_directory(path, parent_fd, state_directory=True)
    finally:
        os.close(parent_fd)
    return path


def peer_identity(sock: socket.socket) -> PeerIdentity:
    """Return kernel-authenticated local peer credentials.

    Args:
        sock: Connected Unix-domain socket.

    Returns:
        The peer's effective user id, group id, and process id.

    Raises:
        SecurityError: If kernel credentials cannot be read.
        RuntimeError: If the current platform is unsupported.
    """
    selected = _ensure_supported_platform()
    if selected == 'linux':
        return _peer_identity_linux(sock)
    return _peer_identity_macos(sock)


def pending_socket_bytes(sock: socket.socket) -> int:
    """Return one snapshot of unread socket bytes without consuming them.

    Args:
        sock: An open stream socket on Linux or macOS.

    Returns:
        Nonnegative kernel receive byte count at the ioctl boundary.

    Raises:
        OSError: The socket cannot be inspected.
    """
    _ensure_supported_platform()
    module = importlib.import_module('fcntl')
    terminal = importlib.import_module('termios')
    count = array.array('i', [0])
    module.ioctl(sock.fileno(), terminal.FIONREAD, count)
    if count[0] < 0:
        raise OSError('socket pending byte count is negative')
    return count[0]


def reap_child(
    process: ChildProcess,
    *,
    timeout: float = 0.0,
) -> int | None:
    """Return a child exit status, waiting only for the requested bound.

    Args:
        process: Child process created by `spawn_child`.
        timeout: Maximum seconds to wait; zero performs a nonblocking poll.

    Returns:
        The exit status, or `None` while the child remains alive.

    Raises:
        ValueError: If `timeout` is negative.
    """
    if timeout < 0:
        raise ValueError('child reap timeout must be nonnegative')
    if timeout == 0:
        return process.poll()
    try:
        return process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


def release_flock(fd: int) -> None:
    """Release the advisory `flock` held through `fd`."""
    _flock(fd, 'LOCK_UN')


def remove_stale_socket(path: Path) -> None:
    """Remove a stale owner-only socket after checking its parent and identity.

    Args:
        path: Socket path to inspect immediately before binding.

    Raises:
        SecurityError: The parent or existing socket is unsafe, live, or
            replaced during inspection.
    """
    parent_fd = _open_parent_directory(path)
    try:
        try:
            metadata = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        if not stat.S_ISSOCK(metadata.st_mode):
            raise SecurityError(f'existing socket path is not a socket: {path}')
        if metadata.st_uid != _effective_uid():
            raise SecurityError(f'existing socket has the wrong owner: {path}')
        if stat.S_IMODE(metadata.st_mode) != FILE_MODE:
            raise SecurityError(f'existing socket must use mode 0600: {path}')
        if _socket_is_live(path):
            raise SecurityError(f'existing socket has a live listener: {path}')
        current = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise SecurityError(f'socket path changed during stale cleanup: {path}')
        os.unlink(path.name, dir_fd=parent_fd)
    finally:
        os.close(parent_fd)


def secure_open_regular_file(path: Path, *, create: bool = True) -> tuple[int, bool]:
    """Open one verified owner-only regular state file, creating it if requested.

    Args:
        path: File path beneath a secure daemon-owned directory.
        create: Whether to create a missing file.

    Returns:
        The open descriptor and whether this call created the file.

    Raises:
        SecurityError: If the file or its parent directory is unsafe.
        FileNotFoundError: If the file is missing and creation is disabled.
    """
    _ensure_supported_platform()
    parent_fd = _open_parent_directory(path)
    flags = os.O_RDWR | os.O_CLOEXEC
    if create:
        flags |= os.O_CREAT
    if hasattr(os, 'O_NOFOLLOW'):
        flags |= os.O_NOFOLLOW
    existed = False
    try:
        try:
            os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            existed = True
        try:
            fd = os.open(path.name, flags, FILE_MODE, dir_fd=parent_fd)
        except OSError as exc:
            if isinstance(exc, FileNotFoundError) and not create:
                raise
            if exc.errno == errno.ELOOP:
                raise SecurityError(f'state file is a symlink: {path}') from exc
            raise SecurityError(f'cannot securely open state file {path}: {exc}') from exc
    finally:
        os.close(parent_fd)
    try:
        _verify_regular_file(path, os.fstat(fd))
    except BaseException:
        os.close(fd)
        raise
    return fd, not existed


def acquire_lock(path: Path) -> FileLock:
    """Acquire an exclusive non-inherited process lock.

    Args:
        path: Owner-only lock file beneath a secure state directory.

    Returns:
        The held lock, which remains active until closed.

    Raises:
        AlreadyLockedError: If another process or descriptor owns the lock.
        SecurityError: If the lock path is not an owner-only regular file.
    """
    fd, _ = secure_open_regular_file(path)
    try:
        try:
            _flock(fd, 'LOCK_EX', 'LOCK_NB')
        except BlockingIOError as exc:
            os.lseek(fd, 0, os.SEEK_SET)
            owner = os.read(fd, 64).decode(errors='replace').strip() or 'unknown'
            raise AlreadyLockedError(f'{path} is already locked by process {owner}') from exc
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode())
        os.fsync(fd)
        return FileLock(fd, path)
    except BaseException:
        os.close(fd)
        raise


def spawn_child(
    argv: Collection[str],
    *,
    pass_fds: Collection[int] = (),
    env: Mapping[str, str] | None = None,
) -> ChildProcess:
    """Exec one child with closed descriptors and an exact allowlist.

    Args:
        argv: Complete non-shell argument vector.
        pass_fds: Exact descriptors that survive the exec boundary.
        env: Optional complete child environment.

    Returns:
        The running child process.

    Raises:
        ValueError: If the argument vector or descriptor allowlist is invalid.
    """
    if not argv or any(not value for value in argv):
        raise ValueError('child argv must contain nonempty strings')
    if any(type(fd) is not int or fd < 0 for fd in pass_fds) or len(set(pass_fds)) != len(
        pass_fds
    ):
        raise ValueError('child descriptor allowlist is invalid')
    return subprocess.Popen(  # noqa: S603
        list(argv),
        close_fds=True,
        env=env,
        pass_fds=pass_fds,
    )


def terminate_child(
    process: ChildProcess,
    *,
    force: bool = False,
) -> None:
    """Request graceful or forced termination of one running child.

    Args:
        process: Child process created by `spawn_child`.
        force: Kill immediately instead of requesting normal termination.
    """
    if process.poll() is not None:
        return
    if force:
        process.kill()
    else:
        process.terminate()


def try_flock_exclusive(fd: int) -> bool:
    """Try to take an exclusive advisory lock without blocking.

    The lock belongs to the open file description, so two descriptors opened
    separately conflict even within one process.

    Args:
        fd: Descriptor of an open lock file.

    Returns:
        `True` when this call acquired the lock, `False` when another open file
        description holds it.
    """
    try:
        _flock(fd, 'LOCK_EX', 'LOCK_NB')
    except BlockingIOError:
        return False
    return True


def validate_secure_directory(path: Path) -> None:
    """Check an existing owner-only directory without creating or changing it.

    Args:
        path: Directory whose final component must not be a symlink.

    Raises:
        SecurityError: The directory or its parent is unsafe, or cannot be
            securely opened.
        FileNotFoundError: The directory or its parent does not exist.
    """
    _ensure_supported_platform()
    parent_fd = _open_parent_directory(path)
    try:
        _open_and_verify_directory(path, parent_fd, state_directory=False)
    finally:
        os.close(parent_fd)


def local_socket_path_candidate(
    state_dir: Path,
    kind: str,
    identity: Iterable[str],
) -> Path:
    """Calculate an ordinary or identity-hashed Unix socket path.

    Args:
        state_dir: Configured daemon state directory for the owning component.
        kind: Socket kind used in the ordinary and fallback filenames.
        identity: Structured stable identity included in the fallback digest.

    Returns:
        Ordinary state path or platform-compatible hashed fallback path.

    Raises:
        SecurityError: If `kind` is invalid or no existing private runtime root
            provides a short enough fallback path.
    """
    if _KIND_PATTERN.fullmatch(kind) is None:
        raise SecurityError(f'invalid socket kind: {kind!r}')
    ordinary = state_dir / _socket_path_name(kind)
    limit = _unix_socket_path_limit()
    if len(os.fsencode(ordinary)) <= limit:
        return ordinary
    digest = _socket_digest(state_dir, kind, identity)
    kind_prefix = kind[0]
    for runtime_root in (os.environ.get('XDG_RUNTIME_DIR', ''), None):
        if runtime_root == '':
            continue
        try:
            root = Path(tempfile.gettempdir() if runtime_root is None else runtime_root)
            if not root.is_absolute():
                continue
            validate_secure_directory(root)
        except (OSError, SecurityError):
            continue
        fallback_dir = root / f'sg-event-daemon-{_effective_uid()}'
        result = fallback_dir / f'{kind_prefix}-{digest}.sock'
        if len(os.fsencode(result)) <= limit:
            return result
    raise SecurityError(
        'no private runtime directory provides a short enough Unix socket path; '
        'use a shorter state path or set XDG_RUNTIME_DIR to an existing, '
        'user-owned mode-0700 directory'
    )


def local_socket_path(
    state_dir: Path,
    kind: str,
    identity: Iterable[str],
) -> Path:
    """Return a secure ordinary or identity-hashed Unix socket path.

    Args:
        state_dir: Secure daemon state directory for the owning component.
        kind: Socket kind used in the ordinary and fallback filenames.
        identity: Structured stable identity included in the fallback digest.

    Returns:
        A socket path whose stale same-user predecessor has been removed.

    Raises:
        SecurityError: If the state, fallback, or existing socket path is
            unsafe.
    """
    result = local_socket_path_candidate(state_dir, kind, identity)
    ensure_secure_state_dir(state_dir)
    if result.parent != state_dir:
        ensure_secure_state_dir(result.parent)
    remove_stale_socket(result)
    return result
