"""Single-writer run lock shared by every command that mutates state."""

from __future__ import annotations

import errno
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from filelock import FileLock, Timeout


class RunBusy(RuntimeError):
    """Another catalogue run holds the lock."""


class _RunFileLock(FileLock):
    """Keep FileLock's reentrant API without letting it truncate an aliased file."""

    def _acquire(self) -> None:
        import fcntl

        path = Path(self.lock_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        mode = self._context.mode
        if mode < 0:
            mode = 0o666
        try:
            fd = os.open(self.lock_file, flags, mode)
        except PermissionError:
            if not path.exists():
                raise
            try:
                fd = os.open(self.lock_file, flags & ~os.O_CREAT, mode)
            except FileNotFoundError:
                return
        except FileNotFoundError:
            if self.lock_file and path.parent.exists():
                return
            raise

        mark_pending = getattr(self, "_mark_descriptor_pending", None)
        try:
            if mark_pending is not None:
                mark_pending(fd)
            opened = os.fstat(fd)
            self._validate_lock_file(opened, "catalogue lock must be a single-link regular file")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK}:
                    self._close_unowned(fd)
                    return
                raise
            opened = os.fstat(fd)
            self._validate_lock_file(opened, "catalogue lock path changed while acquiring its lock")
            mark_owned = getattr(self, "_mark_descriptor_owned", None)
            if mark_owned is None:
                self._context.lock_file_fd = fd
            else:
                mark_owned(fd, (opened.st_dev, opened.st_ino))
        except BaseException:
            if not self.is_locked:
                self._close_unowned(fd)
            raise

    def _release(self) -> None:
        fd = self._context.lock_file_fd
        if fd is None:
            return
        error = None
        try:
            self._validate_lock_file(
                os.fstat(fd), "catalogue lock path changed during the locked operation"
            )
        except (OSError, ValueError) as exc:
            error = exc
        super()._release()
        if error is not None:
            raise error

    def _validate_lock_file(self, opened, message: str) -> None:
        current = os.lstat(self.lock_file)
        if not (
            stat.S_ISREG(opened.st_mode)
            and opened.st_nlink == 1
            and stat.S_ISREG(current.st_mode)
            and current.st_nlink == 1
            and (current.st_dev, current.st_ino) == (opened.st_dev, opened.st_ino)
        ):
            raise ValueError(message)

    def _close_unowned(self, fd: int) -> None:
        mark_released = getattr(self, "_mark_descriptor_released", None)
        if mark_released is not None:
            mark_released()
        else:
            self._context.lock_file_fd = None
        os.close(fd)


@contextmanager
def run_lock(path: Path, timeout: float = 0.0) -> Iterator[None]:
    """Hold an OS file lock for the duration of a run. Fails fast by default."""
    lock = _RunFileLock(os.path.abspath(path), is_singleton=True)
    try:
        lock.acquire(timeout=timeout)
    except Timeout as exc:
        raise RunBusy(f"another catalogue run holds {path}") from exc
    try:
        yield
    finally:
        lock.release()
