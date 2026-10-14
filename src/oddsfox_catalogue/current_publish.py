"""Verified current snapshots with a durable, authoritative publication pointer."""

import hashlib
import json
import os
import re
import shutil
import stat
import time
from contextlib import ExitStack
from pathlib import Path
from uuid import uuid4

import duckdb

from .capture.writer import _fsync_dir, atomic_write_bytes, read_regular_bytes
from .current_config import CurrentSettings, admit, memory_connection, safe_path, validate_root
from .current_contract import CONTRACT, KEYS, PROJECTION_VERSION, SCHEMAS, CurrentError, projection
from .current_store import generation, summary, validate_store
from .fingerprints import semantic_fingerprint
from .limits import directory_bytes


def _check_deadline(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise CurrentError("publication deadline exhausted")


def _json(path):
    return json.loads(read_regular_bytes(path, max_bytes=4 * 1024**2))


def _physical(path):
    safe_path(path)
    digest = hashlib.sha256()
    size = 0
    # Check regular-file status before opening; special files must never block reads.
    if not stat.S_ISREG(path.stat().st_mode):
        raise CurrentError("release contains a special file")
    with path.open("rb") as handle:
        while block := handle.read(1024**2):
            digest.update(block)
            size += len(block)
    return {"sha256": digest.hexdigest(), "bytes": size}


def _parquet(path):
    return "read_parquet('" + str(path).replace("'", "''") + "')"


def _integrity(conn, sources):
    for table, source in sources.items():
        keys = ", ".join(KEYS[table])
        nulls = " OR ".join(f'"{key}" IS NULL' for key in KEYS[table])
        if conn.execute(f"SELECT 1 FROM {source} WHERE {nulls} LIMIT 1").fetchone():
            raise CurrentError(f"null {table} key")
        if conn.execute(
            f"SELECT 1 FROM {source} GROUP BY {keys} HAVING count(*)>1 LIMIT 1"
        ).fetchone():
            raise CurrentError(f"duplicate {table} key")
    events, markets, links, outcomes = (sources[t] for t in SCHEMAS)
    checks = [
        f"SELECT 1 FROM {links} l LEFT JOIN {events} e USING(venue,event_id) LEFT JOIN {markets} m USING(venue,market_id) WHERE e.event_id IS NULL OR m.market_id IS NULL",
        f"SELECT 1 FROM {outcomes} o LEFT JOIN {markets} m USING(venue,market_id) WHERE m.market_id IS NULL OR m.usable IS DISTINCT FROM true",
        f"SELECT 1 FROM {outcomes} GROUP BY venue,asset_kind,asset_id HAVING count(DISTINCT market_id)>1",
        f"SELECT 1 FROM {outcomes} WHERE asset_kind IS NULL OR asset_id IS NULL",
    ]
    for query in checks:
        if conn.execute(query + " LIMIT 1").fetchone():
            raise CurrentError("release relational integrity violation")


def verify_release(path, *, connection=None, settings=None):
    """Recompute content and hashes; optionally prove equality to the source store."""
    path = safe_path(Path(path))
    manifest = _json(path / "manifest.json")
    if (
        manifest.get("contract") != CONTRACT
        or manifest.get("projection_version") != PROJECTION_VERSION
    ):
        raise CurrentError("unsupported release contract")
    expected = {f"{table}.parquet" for table in SCHEMAS} | {"manifest.json"}
    if {child.name for child in path.iterdir()} != expected:
        raise CurrentError("release must contain exactly four Parquets and its manifest")
    if set(manifest.get("relations", {})) != set(SCHEMAS):
        raise CurrentError("invalid release relations")
    owned = connection is None
    stack = ExitStack()
    if owned:
        root = next((p for p in path.parents if (p / "catalogue.json").exists()), None)
        if root is None:
            raise CurrentError("verification requires a confined catalogue root")
        conn = stack.enter_context(memory_connection(settings or CurrentSettings(root)))
    else:
        conn = connection
    try:
        sources = {}
        for table, schema in SCHEMAS.items():
            file = path / f"{table}.parquet"
            recorded = manifest["relations"][table]
            if _physical(file) != {key: recorded.get(key) for key in ("sha256", "bytes")}:
                raise CurrentError(f"corrupt {table} Parquet")
            source = _parquet(file)
            actual = conn.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
            if tuple((row[0], row[1]) for row in actual) != schema:
                raise CurrentError(f"invalid {table} schema")
            fingerprint = semantic_fingerprint(conn, f"SELECT {projection(table)} FROM {source}")
            if any(recorded.get(key) != value for key, value in fingerprint.items()):
                raise CurrentError(f"invalid {table} semantic digest")
            if manifest.get("counts", {}).get(table) != fingerprint["rows"]:
                raise CurrentError(f"invalid {table} count")
            if not owned:
                source_digest = semantic_fingerprint(
                    conn, f"SELECT {projection(table)} FROM {table}"
                )
                if source_digest != fingerprint:
                    raise CurrentError(f"{table} export differs from source store")
            sources[table] = source
        _integrity(conn, sources)
        if not owned:
            validate_store(conn)
            if manifest.get("generation") != generation(conn):
                raise CurrentError("release generation differs from source store")
        return {**manifest, "valid": True, "path": str(path)}
    finally:
        stack.close()


def _pointer(settings):
    pointer = _json(settings.published_dir / "current.json")
    release_id = pointer.get("release_id")
    if (
        pointer.get("contract") != CONTRACT
        or pointer.get("projection_version") != PROJECTION_VERSION
        or not isinstance(release_id, str)
        or not release_id
        or any(c not in "0123456789abcdef" for c in release_id)
        or len(release_id) != 32
    ):
        raise CurrentError("invalid current release pointer")
    return pointer


def verify_current(settings, *, connection=None):
    validate_root(settings.root)
    pointer = _pointer(settings)
    path = settings.published_dir / "releases" / pointer["release_id"]
    if _physical(path / "manifest.json")["sha256"] != pointer.get("manifest_sha256"):
        raise CurrentError("current manifest hash mismatch")
    # The working store can legitimately be ahead after a failed publication.
    manifest = _json(path / "manifest.json")
    if manifest.get("release_id") != pointer["release_id"]:
        raise CurrentError("current release identity mismatch")
    same_generation = connection is not None and manifest.get("generation") == generation(
        connection
    )
    result = verify_release(
        path, connection=connection if same_generation else None, settings=settings
    )
    result["published_generation"] = result["generation"]
    if connection is not None:
        result["store_generation"] = generation(connection)
    return result


def _export(conn, table, destination):
    conn.execute(
        f"COPY (SELECT {projection(table)} FROM {table}) TO ? (FORMAT PARQUET)",
        [str(destination)],
    )


def _cleanup(settings, keep):
    """Delete only generated artifacts after their pointer ownership is settled."""
    for directory, prefix in (
        (settings.published_dir / "releases", ""),
        (settings.published_dir, ".staging-"),
    ):
        if not directory.exists():
            continue
        for child in directory.iterdir():
            name = child.name.removeprefix(prefix)
            if (
                not child.name.startswith(prefix)
                or not re.fullmatch(r"[0-9a-f]{32}", name)
                or name in keep
            ):
                continue
            safe_path(child)
            directory_bytes(child)
            if child.is_dir():
                shutil.rmtree(child)
        _fsync_dir(directory)


def publish(settings, conn, *, deadline=None):
    """Publish one read snapshot. The caller must hold the catalogue writer lock."""
    validate_root(settings.root)
    _check_deadline(deadline)
    published = safe_path(settings.published_dir)
    releases = safe_path(published / "releases")
    current = safe_path(published / "current.json")
    previous = None
    pinned = _pointer(settings) if current.exists() else {}
    _cleanup(settings, {pinned.get("release_id"), pinned.get("previous_release_id")})
    conn.execute("BEGIN TRANSACTION")
    try:
        validate_store(conn)
        if current.exists():
            pointer = _pointer(settings)
            try:
                verified = verify_current(settings, connection=conn)
            except (CurrentError, OSError, ValueError, duckdb.Error):
                pass
            else:
                previous = pointer["release_id"]
                if verified["generation"] == generation(conn):
                    _check_deadline(deadline)
                    conn.execute("COMMIT")
                    return {**verified, "noop": True}
        # Existing failed staging and orphan releases are counted by admit as retained bytes.
        estimate = 2 * (settings.store_path.stat().st_size if settings.store_path.exists() else 0)
        wal = safe_path(Path(str(settings.store_path) + ".wal"))
        if wal.exists():
            estimate += 2 * wal.stat().st_size
        if published.exists():
            estimate += sum(_physical(p)["bytes"] for p in published.rglob("*.parquet"))
        admit(settings, additional_bytes=max(1024**2, estimate))
        releases.mkdir(parents=True, exist_ok=True)
        release_id = uuid4().hex
        stage = safe_path(published / f".staging-{release_id}")
        stage.mkdir()
        state = summary(conn)
        manifest = {
            "contract": CONTRACT,
            "projection_version": PROJECTION_VERSION,
            "release_id": release_id,
            "generation": generation(conn),
            "counts": state["counts"],
            "coverage": {
                "complete": False,
                "description": "Observed current state; full upstream coverage is not asserted.",
            },
            "backlog": {
                key: value for key, value in state.items() if key not in {"generation", "counts"}
            },
            "relations": {},
        }
        for table in SCHEMAS:
            _check_deadline(deadline)
            file = stage / f"{table}.parquet"
            _export(conn, table, file)
            _check_deadline(deadline)
            manifest["relations"][table] = {
                **semantic_fingerprint(conn, f"SELECT {projection(table)} FROM {_parquet(file)}"),
                **_physical(file),
            }
            with file.open("rb") as handle:
                os.fsync(handle.fileno())
        atomic_write_bytes(stage / "manifest.json", json.dumps(manifest, sort_keys=True).encode())
        verify_release(stage, connection=conn)
        _check_deadline(deadline)
        admit(settings)
        _fsync_dir(stage)
        final = releases / release_id
        os.replace(stage, final)
        _fsync_dir(releases)
        _fsync_dir(published)
        _check_deadline(deadline)
        pointer = {
            "contract": CONTRACT,
            "projection_version": PROJECTION_VERSION,
            "release_id": release_id,
            "previous_release_id": previous,
            "manifest_sha256": _physical(final / "manifest.json")["sha256"],
        }
        pending_pointer = safe_path(published / "current.next.json")
        atomic_write_bytes(pending_pointer, json.dumps(pointer, sort_keys=True).encode())
        _check_deadline(deadline)
        os.replace(pending_pointer, current)
        _fsync_dir(published)
        conn.execute("COMMIT")
        result = {**manifest, "valid": True, "path": str(final), "noop": False}
        try:
            _cleanup(settings, {release_id, previous})
        except Exception as exc:
            # Promotion is the commit point; a cleanup failure cannot undo a valid pointer.
            result["retention_warning"] = str(exc)
        return result
    except BaseException:
        conn.execute("ROLLBACK")
        raise
