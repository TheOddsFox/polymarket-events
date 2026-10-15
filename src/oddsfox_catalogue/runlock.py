"""Single-writer run lock shared by every command that mutates state."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from filelock import FileLock, Timeout


class RunBusy(RuntimeError):
    """Another catalogue run holds the lock."""


@contextmanager
def run_lock(path: Path, timeout: float = 0.0) -> Iterator[None]:
    """Hold an OS file lock for the duration of a run. Fails fast by default."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(path), is_singleton=True)
    try:
        lock.acquire(timeout=timeout)
    except Timeout as exc:
        raise RunBusy(f"another catalogue run holds {path}") from exc
    try:
        yield
    finally:
        lock.release()
