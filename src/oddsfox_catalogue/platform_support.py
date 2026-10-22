"""POSIX filesystem and process primitives used by the catalogue runtime."""

from __future__ import annotations

import os
import shutil
import signal
from os import PathLike


def peak_rss_bytes() -> int:
    """Return this process' peak resident set size, or fail if unavailable."""
    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if os.uname().sysname == "Darwin" else peak * 1024)


def operator_signals(*, include_interrupt: bool = False) -> tuple[signal.Signals, ...]:
    """Return only termination signals provided by this Python platform."""
    names = ["SIGTERM"]
    if hasattr(signal, "SIGHUP"):
        names.append("SIGHUP")
    if include_interrupt and hasattr(signal, "SIGINT"):
        names.append("SIGINT")
    return tuple(getattr(signal, name) for name in names)


def is_symlink(path: str | PathLike[str]) -> bool:
    return os.path.islink(path)


def open_regular_nofollow(path: str | PathLike[str], flags: int, mode: int = 0o600) -> int:
    """Open a regular file without following a final symlink."""
    return os.open(path, flags | getattr(os, "O_NOFOLLOW", 0), mode)


def sync_directory(path: str | PathLike[str]) -> None:
    """Persist directory changes after atomic renames."""
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def remove_file(path: str | PathLike[str]) -> None:
    os.unlink(path)


def remove_tree(path: str | PathLike[str]) -> None:
    shutil.rmtree(path)


def replace_file(source: str | PathLike[str], destination: str | PathLike[str]) -> None:
    os.replace(source, destination)


def replace_directory(source: str | PathLike[str], destination: str | PathLike[str]) -> None:
    os.replace(source, destination)
