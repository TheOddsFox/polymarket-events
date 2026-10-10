"""Point-in-time backups with a checksummed manifest.

A backup holds everything needed to rebuild the catalogue without Gamma:

- ``ledger.sqlite``: the capture ledger, copied with SQLite's online backup API.
- ``catalogue.duckdb``: the warehouse, checkpointed first so the file is complete.
- ``raw/``: immutable raw pages and manifests (copied, never rewritten).
- ``metadata/``: targeted metadata captures and repo-local handoff bundles.
- ``published/``: releases and the ``current.json`` pointer.
- ``dlt_pipelines/``: dlt state, so loads resume where they stopped.
- ``backup.json``: SHA-256 and size of every file above, plus the git SHA it was taken at.

Run ``verify_backup`` on a copy before relying on it. It re-hashes every file.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.ids import iso_utc, utc_now
from oddsfox_catalogue.runlock import current_git_sha

MANIFEST = "backup.json"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_tree(source: Path, dest: Path) -> None:
    if source.exists():
        shutil.copytree(source, dest, copy_function=shutil.copy2)
    else:
        dest.mkdir(parents=True)


def _snapshot_ledger(source: Path, dest: Path) -> None:
    src = sqlite3.connect(source)
    try:
        dst = sqlite3.connect(dest)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def _checkpoint_warehouse(path: Path) -> None:
    connection = duckdb.connect(str(path))
    try:
        connection.execute("CHECKPOINT")
    finally:
        connection.close()


def create_backup(
    settings: Settings,
    *,
    dest_root: Path | None = None,
    now: datetime | None = None,
) -> Path:
    """Write a new backup directory and return its path. Caller holds the run lock."""
    when = now or utc_now()
    stamp = when.strftime("%Y%m%dT%H%M%SZ")
    dest = (dest_root or settings.data_dir / "backups") / stamp
    if dest.exists():
        raise FileExistsError(f"backup {dest} already exists")
    dest.mkdir(parents=True)

    if settings.ledger_path.exists():
        _snapshot_ledger(settings.ledger_path, dest / "ledger.sqlite")
    if settings.warehouse_path.exists():
        _checkpoint_warehouse(settings.warehouse_path)
        shutil.copy2(settings.warehouse_path, dest / "catalogue.duckdb")
    _copy_tree(settings.raw_dir, dest / "raw")
    _copy_tree(settings.data_dir / "metadata", dest / "metadata")
    _copy_tree(settings.published_dir, dest / "published")
    _copy_tree(settings.dlt_pipelines_dir, dest / "dlt_pipelines")

    files: dict[str, dict[str, Any]] = {}
    for path in sorted(p for p in dest.rglob("*") if p.is_file()):
        relative = path.relative_to(dest).as_posix()
        files[relative] = {"sha256": _sha256(path), "bytes": path.stat().st_size}
    manifest = {
        "created_at": iso_utc(when),
        "git_sha": current_git_sha(settings.root),
        "files": files,
    }
    (dest / MANIFEST).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return dest


def verify_backup(backup_dir: Path) -> list[str]:
    """Return a list of problems. An empty list means every file matches its checksum."""
    manifest_path = backup_dir / MANIFEST
    if not manifest_path.exists():
        return [f"{MANIFEST} is missing"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    problems: list[str] = []
    for relative, expected in manifest["files"].items():
        path = backup_dir / relative
        if not path.exists():
            problems.append(f"missing {relative}")
        elif _sha256(path) != expected["sha256"]:
            problems.append(f"checksum mismatch {relative}")
    listed = set(manifest["files"]) | {MANIFEST}
    for path in backup_dir.rglob("*"):
        if path.is_file() and path.relative_to(backup_dir).as_posix() not in listed:
            problems.append(f"unlisted file {path.relative_to(backup_dir).as_posix()}")
    return problems
