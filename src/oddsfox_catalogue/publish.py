"""Immutable Parquet releases and the atomic ``current.json`` pointer.

A release is written in three steps, so a reader never sees a partial one:

1. Export every table to Parquet (zstd) and write ``release.json`` into a staging directory.
2. Rename the staging directory to ``releases/<release_id>``. The name is unique, so nothing
   is overwritten.
3. Write ``current.json`` to a temporary file and ``os.replace`` it over the old pointer. That
   replace is the only moment readers switch releases.

A crash before step 3 leaves an unreferenced release, which retention prunes later. The
previous release stays current the whole time.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.faults import fault_point
from oddsfox_catalogue.ids import iso_utc, utc_now
from oddsfox_catalogue.warehouse import BaselineMissing

PROJECTION_TABLE_QUERIES: dict[str, str] = {
    "events": "SELECT * FROM marts.mart_event_catalogue ORDER BY venue, event_id",
    "markets": "SELECT * FROM core.markets_current ORDER BY venue, market_id",
    "outcomes": "SELECT * FROM core.outcomes_current ORDER BY venue, market_id, outcome_index",
    "event_tags": "SELECT * FROM core.event_tags_current ORDER BY venue, event_id, tag_id",
    "event_series": "SELECT * FROM core.event_series_current ORDER BY venue, event_id, series_id",
    "market_event_bridge": (
        "SELECT * FROM core.market_event_bridge ORDER BY venue, market_id, event_id"
    ),
}

CURRENT_POINTER = "current.json"


class PublishBlocked(RuntimeError):
    """A release must not be published: the warehouse is empty or fails a safety check."""


@dataclass
class ReleaseInfo:
    release_id: str
    path: Path
    tables: dict[str, dict[str, Any]] = field(default_factory=dict)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fingerprint_parquet(connection: duckdb.DuckDBPyConnection, path: Path) -> dict[str, Any]:
    """Row count and an order-independent content hash. Equal inputs give equal fingerprints."""
    # hash(r) hashes the whole row as one struct, so each row gets one hash.
    row = connection.execute(
        "SELECT count(*), sum(hash(r)), bit_xor(hash(r)) FROM read_parquet(?) AS r",
        [str(path)],
    ).fetchone()
    assert row is not None
    rows, total, xor = row
    return {"rows": int(rows), "sum_hash": int(total or 0), "xor_hash": int(xor or 0)}


def release_id_for(now: datetime, existing: set[str]) -> str:
    base = now.strftime("%Y%m%dT%H%M%SZ")
    candidate, suffix = base, 1
    while candidate in existing:
        suffix += 1
        candidate = f"{base}-{suffix}"
    return candidate


def _read_current(published: Path) -> dict[str, Any] | None:
    pointer = published / CURRENT_POINTER
    if not pointer.exists():
        return None
    return json.loads(pointer.read_text(encoding="utf-8"))


def _check_publishable(connection: duckdb.DuckDBPyConnection) -> None:
    """Refuse to publish an empty catalogue or one with misaligned outcomes."""
    try:
        events = connection.execute("SELECT count(*) FROM marts.mart_event_catalogue").fetchone()
        quarantined = connection.execute(
            "SELECT count(*) FROM core.quarantine_market_outcomes"
        ).fetchone()
    except duckdb.Error as exc:
        raise BaselineMissing(f"warehouse has no built marts yet: {exc}") from exc
    assert events is not None and quarantined is not None
    if events[0] == 0:
        raise PublishBlocked("mart_event_catalogue is empty; refusing to publish")
    if quarantined[0] != 0:
        raise PublishBlocked(f"{quarantined[0]} misaligned markets; refusing to publish")


def publish_release(
    settings: Settings,
    *,
    now: datetime | None = None,
    git_sha: str | None = None,
) -> ReleaseInfo:
    """Write a new immutable release and point ``current.json`` at it."""
    when = now or utc_now()
    # Check before creating anything, so a blocked publish leaves no directories behind.
    if not settings.warehouse_path.exists():
        raise BaselineMissing(f"no warehouse at {settings.warehouse_path}; run bootstrap first")
    published = settings.published_dir
    releases = published / "releases"
    releases.mkdir(parents=True, exist_ok=True)
    existing = {p.name for p in releases.iterdir() if p.is_dir() and not p.name.startswith(".")}
    release_id = release_id_for(when, existing)
    staging = releases / f".staging-{release_id}"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir()

    try:
        manifest_bytes = _export_release(settings, staging, release_id, when, git_sha)
    except BaseException:
        # A failed export must not leave a half-written staging directory behind.
        shutil.rmtree(staging, ignore_errors=True)
        raise

    final = releases / release_id
    os.rename(staging, final)
    fault_point("mid_publish")

    pointer = {
        "release_id": release_id,
        "path": f"releases/{release_id}",
        "published_at": iso_utc(when),
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
    }
    temporary = published / f"{CURRENT_POINTER}.tmp"
    temporary.write_text(json.dumps(pointer, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(temporary, published / CURRENT_POINTER)

    prune_releases(settings, keep=settings.publish.retain_releases)
    tables = json.loads(manifest_bytes)["tables"]
    return ReleaseInfo(release_id=release_id, path=final, tables=tables)


def _export_release(
    settings: Settings,
    staging: Path,
    release_id: str,
    when: datetime,
    git_sha: str | None,
) -> bytes:
    """Export every projection into ``staging`` and write ``release.json``. Returns its bytes."""
    tables: dict[str, dict[str, Any]] = {}
    connection = duckdb.connect(str(settings.warehouse_path), read_only=True)
    try:
        _check_publishable(connection)
        for name, query in PROJECTION_TABLE_QUERIES.items():
            target = staging / f"{name}.parquet"
            connection.execute(f"COPY ({query}) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            tables[name] = {
                "file": target.name,
                "sha256": _sha256_file(target),
                **fingerprint_parquet(connection, target),
            }
        registered = connection.execute(
            "SELECT batch_id FROM bronze.batch_registry ORDER BY batch_id"
        ).fetchall()
    finally:
        connection.close()

    manifest = {
        "release_id": release_id,
        "created_at": iso_utc(when),
        "git_sha": git_sha,
        "projection_version": "v1",
        "batch_ids": [row[0] for row in registered],
        "tables": tables,
    }
    manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")
    (staging / "release.json").write_bytes(manifest_bytes)
    return manifest_bytes


def prune_releases(settings: Settings, *, keep: int) -> list[str]:
    """Delete all but the newest ``keep`` releases. The current release is never removed."""
    published = settings.published_dir
    releases = published / "releases"
    if not releases.exists():
        return []
    current = (_read_current(published) or {}).get("release_id")
    names = sorted(p.name for p in releases.iterdir() if p.is_dir() and not p.name.startswith("."))
    protected = set(names[-keep:]) if keep > 0 else set()
    if current:
        protected.add(current)
    removed: list[str] = []
    for name in names:
        if name not in protected:
            shutil.rmtree(releases / name)
            removed.append(name)
    for stale in releases.glob(".staging-*"):
        shutil.rmtree(stale)
    return removed


def current_release(settings: Settings) -> dict[str, Any] | None:
    return _read_current(settings.published_dir)
