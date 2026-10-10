#!/usr/bin/env python3
"""Audit wheel/sdist inventories without extracting or importing their contents."""

from __future__ import annotations

import argparse
import json
import re
import stat
import tarfile
import zipfile
from pathlib import Path, PurePosixPath


def allowed_source(path: str) -> bool:
    name = PurePosixPath(path)
    if name.suffix == ".py" and name.parent.as_posix() in {
        "oddsfox_catalogue",
        "oddsfox_catalogue/capture",
        "oddsfox_catalogue/gamma",
        "oddsfox_catalogue/load",
        "oddsfox_catalogue/orchestration",
    }:
        return True
    parts = name.parts
    if parts[:2] != ("oddsfox_catalogue", "dbt"):
        return False
    if len(parts) == 3:
        return parts[2] in {"dbt_project.yml", "profiles.yml"}
    return (
        parts[2] == "models"
        and name.suffix in {".sql", ".yml"}
        or parts[2] in {"macros", "tests"}
        and name.suffix == ".sql"
    )


def audit(path: Path) -> dict[str, object]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 32 * 1024**2:
        raise ValueError("distribution must be a bounded regular file")
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            members = [
                (i.filename, i.file_size, not i.is_dir(), (i.external_attr >> 16) & 0o170000)
                for i in archive.infolist()
            ]
        prefix = ""
    elif path.name.endswith(".tar.gz"):
        members = []
        total = 0
        with tarfile.open(path, mode="r|gz") as archive:
            for member in archive:
                total += member.size
                if member.size > 2 * 1024**2 or total > 32 * 1024**2 or len(members) >= 10_000:
                    raise ValueError("distribution exceeds unpacked allowance")
                members.append(
                    (
                        member.name,
                        member.size,
                        member.isfile(),
                        stat.S_IFREG if member.isfile() else stat.S_IFDIR if member.isdir() else -1,
                    )
                )
        prefix = path.name[:-7] + "/"
    else:
        raise ValueError("expected a wheel or gzip sdist")
    seen: set[str] = set()
    if len(members) > 10_000:
        raise ValueError("distribution has too many members")
    total = 0
    for original, size, is_file, mode in members:
        name = PurePosixPath(original)
        if (
            name.is_absolute()
            or ".." in name.parts
            or "\\" in original
            or original in seen
            or mode not in {0, stat.S_IFREG, stat.S_IFDIR}
        ):
            raise ValueError("unsafe or duplicate distribution member")
        seen.add(original)
        if not is_file:
            continue
        if prefix and not original.startswith(prefix):
            raise ValueError("sdist member leaves its declared prefix")
        relative = original[len(prefix) :]
        if prefix:
            allowed = (
                relative
                in {
                    "pyproject.toml",
                    "uv.lock",
                    "README.md",
                    "LICENSE",
                    ".gitignore",
                    "PKG-INFO",
                    "config/catalogue.toml",
                }
                or relative.startswith("docs/")
                and relative.endswith(".md")
                or relative.startswith("src/")
                and allowed_source(relative[4:])
            )
        else:
            allowed = (
                allowed_source(relative)
                or re.fullmatch(
                    r"oddsfox_catalogue-[^/]+\.dist-info/(?:METADATA|WHEEL|entry_points\.txt|RECORD|licenses/LICENSE)",
                    relative,
                )
                is not None
            )
        if not allowed:
            raise ValueError(f"unexpected distribution member: {relative}")
        total += size
        if size > 2 * 1024**2 or total > 32 * 1024**2:
            raise ValueError("distribution exceeds unpacked byte allowance")
    required = {
        f"{prefix}{'src/' if prefix else ''}oddsfox_catalogue/{name}"
        for name in ("__init__.py", "cli.py", "dbt/dbt_project.yml", "dbt/profiles.yml")
    }
    if not required <= seen:
        raise ValueError("distribution lacks required installed resources")
    return {"file": path.name, "members": len(seen), "unpacked_bytes": total}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archives", type=Path, nargs="+")
    args = parser.parse_args()
    print(json.dumps([audit(path) for path in args.archives], sort_keys=True))
