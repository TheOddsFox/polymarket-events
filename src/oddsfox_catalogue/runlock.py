"""Single-writer run lock shared by every command that mutates state."""

from __future__ import annotations

import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from filelock import FileLock, Timeout


class RunBusy(RuntimeError):
    """Another capture, load, dbt, or publish run holds the lock."""


@contextmanager
def run_lock(path: Path, timeout: float = 0.0) -> Iterator[None]:
    """Hold an OS file lock for the duration of a run. Fails fast by default."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(path), timeout=timeout)
    try:
        lock.acquire()
    except Timeout as exc:
        raise RunBusy(f"another catalogue run holds {path}") from exc
    try:
        yield
    finally:
        lock.release()


def current_git_sha(root: Path) -> str | None:
    """HEAD commit of the project, or None outside a git checkout."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    sha = result.stdout.strip()
    return sha or None
