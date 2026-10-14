"""Finite local storage limits, shared by operator entry points."""

import os
import stat
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded
from oddsfox_catalogue.timing import phase_timing, record_counter


@dataclass(frozen=True)
class StorageUsage:
    retained_bytes: int
    temporary_bytes: int


def directory_bytes(directory: Path) -> int:
    return _scan_directory(directory)


def _scan_directory(directory: Path, account: Callable[[str, int], None] | None = None) -> int:
    for ancestor in (directory, *directory.parents):
        if ancestor.is_symlink():
            raise RequestBudgetExceeded("storage path contains a symlink")
    if not directory.exists():
        return 0
    if hasattr(os, "fwalk") and os.stat in os.supports_dir_fd and os.listdir in os.supports_fd:
        return _directory_bytes_dirfd(directory, account)
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
                if account is not None:
                    account(str(path), info.st_size)
            else:
                raise RequestBudgetExceeded("storage contains a symlink or special file")
        except FileNotFoundError:
            # Temporary jobs can disappear while their owning process commits them.
            continue
    return total


def _walk_error(error: OSError) -> None:
    if not isinstance(error, FileNotFoundError):
        raise error


def _directory_bytes_dirfd(
    directory: Path, account: Callable[[str, int], None] | None = None
) -> int:
    try:
        root_info = directory.lstat()
    except FileNotFoundError:
        return 0
    if stat.S_ISREG(root_info.st_mode):
        if account is not None:
            account(str(directory), root_info.st_size)
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
                current_prefix = current.rstrip(os.sep) + os.sep
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
                        if account is not None:
                            account(current_prefix + name, info.st_size)
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


def _union_bytes(paths: tuple[Path, ...]) -> int:
    for path in paths:
        for ancestor in (path, *path.parents):
            if ancestor.is_symlink():
                raise RequestBudgetExceeded("storage path contains a symlink")
    roots = {Path(os.path.abspath(path)) for path in paths}
    roots = {p for p in roots if not any(p != q and p.is_relative_to(q) for q in roots)}
    return sum(directory_bytes(root) for root in roots)


def _retained_paths(settings: Settings) -> tuple[Path, ...]:
    return (
        settings.root,
        settings.data_dir,
        settings.state_dir,
        settings.dlt_pipelines_dir,
        settings.warehouse_path,
        settings.warehouse_path.with_name(settings.warehouse_path.name + ".wal"),
    )


def retained_bytes(settings: Settings) -> int:
    return _union_bytes(_retained_paths(settings))


def temporary_bytes(settings: Settings) -> int:
    return _union_bytes((settings.temporary_dir, settings.dlt_pipelines_dir))


@phase_timing("storage.measure")
def measure_storage(settings: Settings) -> StorageUsage:
    retained_paths = _retained_paths(settings)
    temporary_paths = (settings.temporary_dir, settings.dlt_pipelines_dir)
    for path in (*retained_paths, *temporary_paths):
        for ancestor in (path, *path.parents):
            if ancestor.is_symlink():
                raise RequestBudgetExceeded("storage path contains a symlink")
    retained_roots = {Path(os.path.abspath(path)) for path in retained_paths}
    temporary_roots = {Path(os.path.abspath(path)) for path in temporary_paths}
    retained_roots = {
        p for p in retained_roots if not any(p != q and p.is_relative_to(q) for q in retained_roots)
    }
    temporary_roots = {
        p
        for p in temporary_roots
        if not any(p != q and p.is_relative_to(q) for q in temporary_roots)
    }
    roots = retained_roots | temporary_roots
    roots = {p for p in roots if not any(p != q and p.is_relative_to(q) for q in roots)}
    retained_names = {os.path.normcase(str(root)) for root in retained_roots}
    temporary_names = {os.path.normcase(str(root)) for root in temporary_roots}
    retained_prefixes = tuple(root.rstrip(os.sep) + os.sep for root in retained_names)
    temporary_prefixes = tuple(root.rstrip(os.sep) + os.sep for root in temporary_names)
    retained = temporary = 0

    def account(path: str, size: int) -> None:
        nonlocal retained, temporary
        path = os.path.normcase(path)
        if path in retained_names or path.startswith(retained_prefixes):
            retained += size
        if path in temporary_names or path.startswith(temporary_prefixes):
            temporary += size

    record_counter("storage.walks", len(roots))
    for root in roots:
        _scan_directory(root, account)
    return StorageUsage(retained, temporary)


def remaining_temp_bytes(settings: Settings, *, usage: StorageUsage | None = None) -> int:
    temporary = temporary_bytes(settings) if usage is None else usage.temporary_bytes
    remaining = settings.capture.max_temp_bytes - temporary
    if remaining <= 0:
        raise RequestBudgetExceeded("temporary storage allowance exhausted")
    return remaining


def enforce_storage_limits(
    settings: Settings, *, additional_bytes: int = 0, usage: StorageUsage | None = None
) -> None:
    usage = measure_storage(settings) if usage is None else usage
    if usage.retained_bytes + additional_bytes > settings.capture.max_retained_bytes:
        raise RequestBudgetExceeded("retained storage allowance exhausted")
    if usage.temporary_bytes > settings.capture.max_temp_bytes:
        raise RequestBudgetExceeded("temporary storage allowance exhausted")
