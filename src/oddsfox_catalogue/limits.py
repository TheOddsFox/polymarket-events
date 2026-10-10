"""Finite local storage limits, shared by operator entry points."""

import stat
from pathlib import Path

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded


def directory_bytes(directory: Path) -> int:
    for ancestor in (directory, *directory.parents):
        if ancestor.is_symlink():
            raise RequestBudgetExceeded("storage path contains a symlink")
    if not directory.exists():
        return 0
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


def _union_bytes(paths: tuple[Path, ...]) -> int:
    roots = {path.absolute() for path in paths}
    roots = {p for p in roots if not any(p != q and p.is_relative_to(q) for q in roots)}
    return sum(directory_bytes(root) for root in roots)


def retained_bytes(settings: Settings) -> int:
    return _union_bytes(
        (
            settings.data_dir,
            settings.state_dir,
            settings.dlt_pipelines_dir,
            settings.warehouse_path,
            settings.warehouse_path.with_name(settings.warehouse_path.name + ".wal"),
        )
    )


def temporary_bytes(settings: Settings) -> int:
    return _union_bytes((settings.temporary_dir, settings.dlt_pipelines_dir))


def remaining_temp_bytes(settings: Settings) -> int:
    remaining = settings.capture.max_temp_bytes - temporary_bytes(settings)
    if remaining <= 0:
        raise RequestBudgetExceeded("temporary storage allowance exhausted")
    return remaining


def enforce_storage_limits(settings: Settings, *, additional_bytes: int = 0) -> None:
    if retained_bytes(settings) + additional_bytes > settings.capture.max_retained_bytes:
        raise RequestBudgetExceeded("retained storage allowance exhausted")
    if temporary_bytes(settings) > settings.capture.max_temp_bytes:
        raise RequestBudgetExceeded("temporary storage allowance exhausted")
