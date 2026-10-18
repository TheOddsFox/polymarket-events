"""Small, explicit platform primitives used by the catalogue runtime."""

from __future__ import annotations

import errno
import os
import signal
from contextlib import AbstractContextManager, nullcontext
from os import PathLike
from pathlib import Path


def peak_rss_bytes() -> int:
    """Return this process' peak resident set size, or fail if unavailable."""
    if os.name == "nt":
        from ._windows import peak_rss_bytes as windows_peak_rss_bytes

        return windows_peak_rss_bytes()

    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if os.uname().sysname == "Darwin" else peak * 1024)


def peak_process_rss_bytes(process_or_handle) -> int:
    """Return a live or exited Windows process' peak working set in bytes."""
    if os.name != "nt":
        raise OSError(errno.ENOSYS, "per-process peak RSS is Windows-only")
    from ._windows import peak_process_rss_bytes as windows_peak_process_rss_bytes

    return windows_peak_process_rss_bytes(process_or_handle)


def operator_signals(*, include_interrupt: bool = False) -> tuple[signal.Signals, ...]:
    """Return only termination signals provided by this Python platform."""
    names = ["SIGTERM"]
    if hasattr(signal, "SIGHUP"):
        names.append("SIGHUP")
    if hasattr(signal, "SIGBREAK"):
        names.append("SIGBREAK")
    if include_interrupt and hasattr(signal, "SIGINT"):
        names.append("SIGINT")
    return tuple(getattr(signal, name) for name in names)


def is_link_or_reparse(path: str | PathLike[str]) -> bool:
    if os.name == "nt":
        from ._windows import is_link_or_reparse as windows_is_link_or_reparse

        return windows_is_link_or_reparse(path)
    return os.path.islink(path)


def open_regular_nofollow(path: str | PathLike[str], flags: int, mode: int = 0o600) -> int:
    """Open a regular file without following a final link/reparse point."""
    if os.name == "nt":
        from ._windows import open_regular_nofollow as windows_open_regular_nofollow

        return windows_open_regular_nofollow(path, flags, mode)

    nofollow = getattr(os, "O_NOFOLLOW", 0)
    return os.open(path, flags | nofollow, mode)


def pinned_path(path: str | PathLike[str]) -> AbstractContextManager[Path]:
    """Pin and validate a directory path while a sensitive operation runs."""
    if os.name == "nt":
        from ._windows import pinned_path as windows_pinned_path

        return windows_pinned_path(path)
    return nullcontext(path)


def require_durability(path: str | PathLike[str]) -> None:
    if os.name == "nt":
        from ._windows import require_durability as windows_require_durability

        windows_require_durability(path)


def sync_directory(path: str | PathLike[str]) -> None:
    """Persist directory changes; Windows requires a supported volume flush."""
    if os.name == "nt":
        from ._windows import sync_directory as windows_sync_directory

        windows_sync_directory(path)
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def remove_file(path: str | PathLike[str]) -> None:
    if os.name == "nt":
        from ._windows import remove_file as windows_remove_file

        windows_remove_file(path)
        return
    os.unlink(path)


def remove_tree(path: str | PathLike[str]) -> None:
    if os.name == "nt":
        from ._windows import remove_tree as windows_remove_tree

        windows_remove_tree(path)
        return
    import shutil

    shutil.rmtree(path)


def replace_file(source: str | PathLike[str], destination: str | PathLike[str]) -> None:
    if os.name == "nt":
        from ._windows import replace_file as windows_replace_file

        windows_replace_file(source, destination)
        return
    os.replace(source, destination)


def replace_directory(source: str | PathLike[str], destination: str | PathLike[str]) -> None:
    if os.name == "nt":
        from ._windows import replace_directory as windows_replace_directory

        windows_replace_directory(source, destination)
        return
    os.replace(source, destination)


class WindowsJob:
    """Lazily create a Win32 kill-on-close Job Object."""

    def __new__(cls):
        if os.name != "nt":
            raise OSError("Windows process containment is unavailable on this platform")
        from ._windows import WindowsJob as implementation

        return implementation()
