"""Finite local storage limits, shared by operator entry points."""

import os
import stat
from contextlib import closing
from pathlib import Path

from oddsfox_catalogue.gamma.http import RequestBudgetExceeded


class StorageMeter:
    """One retained-byte walk per invocation, plus bytes added as raw is written."""

    def __init__(self):
        self.retained = None

    def snapshot(self, directory: Path) -> int:
        self.retained = directory_bytes(directory)
        return self.retained

    def add(self, amount: int) -> None:
        if self.retained is not None and amount > 0:
            self.retained += amount


def directory_bytes(directory: Path) -> int:
    for ancestor in (directory, *directory.parents):
        if ancestor.is_symlink():
            raise RequestBudgetExceeded("storage path contains a symlink")
    if not directory.exists():
        return 0
    if hasattr(os, "fwalk") and os.stat in os.supports_dir_fd and os.listdir in os.supports_fd:
        return _directory_bytes_dirfd(directory)
    total = 0
    pending = [directory]
    while pending:
        path = pending.pop()
        try:
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                pending.extend(path.iterdir())
            elif stat.S_ISREG(info.st_mode):
                total += info.st_size
            else:
                raise RequestBudgetExceeded("storage contains a symlink or special file")
        except FileNotFoundError:
            # Temporary jobs can disappear while their owning process commits them.
            continue
    return total


def _walk_error(error: OSError) -> None:
    if not isinstance(error, FileNotFoundError):
        raise error


def _directory_bytes_dirfd(directory: Path) -> int:
    try:
        root_info = directory.lstat()
    except FileNotFoundError:
        return 0
    if stat.S_ISREG(root_info.st_mode):
        return root_info.st_size
    if not stat.S_ISDIR(root_info.st_mode):
        raise RequestBudgetExceeded("storage contains a symlink or special file")
    total = 0
    expected = {directory: (root_info.st_dev, root_info.st_ino)}
    visited = set()
    with closing(
        os.fwalk(directory, topdown=True, onerror=_walk_error, follow_symlinks=False)
    ) as entries:
        try:
            for current, dirnames, _, descriptor in entries:
                current = Path(current)
                info = os.fstat(descriptor)
                if not stat.S_ISDIR(info.st_mode):
                    raise RequestBudgetExceeded("storage contains a symlink or special file")
                visited.add((current, info.st_dev, info.st_ino))
                descend = []
                # fwalk may silently omit entries whose classification raises OSError.
                for name in os.listdir(descriptor):
                    try:
                        info = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    if stat.S_ISDIR(info.st_mode):
                        descend.append(name)
                        expected[current / name] = (info.st_dev, info.st_ino)
                    elif stat.S_ISREG(info.st_mode):
                        total += info.st_size
                    else:
                        raise RequestBudgetExceeded("storage contains a symlink or special file")
                dirnames[:] = descend
        except FileNotFoundError:
            # A temporary root can disappear before fwalk invokes onerror.
            pass
    for path, (device, inode) in expected.items():
        if (path, device, inode) in visited:
            continue
        # fwalk can silently skip a directory replaced by a symlink before descent.
        for ancestor in (path, *path.parents):
            if ancestor.is_symlink():
                raise RequestBudgetExceeded("storage path contains a symlink")
        try:
            path.lstat()
        except FileNotFoundError:
            continue
        raise RequestBudgetExceeded("storage directory changed or was skipped during accounting")
    return total
