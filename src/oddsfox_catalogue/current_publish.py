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
from .current_contract import (
    CONTRACT,
    PROJECTION_VERSION,
    SCHEMAS,
    CurrentError,
    projection,
)
from .current_store import _validate_store, generation, summary
from .current_store import validate_public as _integrity
from .fingerprints import semantic_fingerprint
from .limits import directory_bytes


def _check_deadline(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise CurrentError("publication deadline exhausted")


def _json(path):
    value = json.loads(read_regular_bytes(path, max_bytes=4 * 1024**2))
    if not isinstance(value, dict):
        raise CurrentError("release metadata must be an object")
    return value


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


def verify_release(path, *, connection=None, settings=None):
    """Recompute content and hashes; optionally prove equality to the source store."""
    path = safe_path(Path(path))
    with ExitStack() as stack:
        if connection is None:
            root = next((p for p in path.parents if (p / "catalogue.json").exists()), None)
            if root is None:
                raise CurrentError("verification requires a confined catalogue root")
            connection = stack.enter_context(memory_connection(settings or CurrentSettings(root)))
            return _verify_release_on_connection(path, connection, bind_store=False)
        return _verify_release_on_connection(path, connection, bind_store=True)


def _verify_release_on_connection(path, conn, *, bind_store):
    """Reuse one bounded recovery connection for current or older release evidence."""
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
    relations = manifest.get("relations")
    if (
        not isinstance(relations, dict)
        or set(relations) != set(SCHEMAS)
        or any(not isinstance(value, dict) for value in relations.values())
        or not isinstance(manifest.get("counts"), dict)
    ):
        raise CurrentError("invalid release relations")
    _inspect_relations(path, conn, bind_store=bind_store, expected=manifest)
    if bind_store and manifest.get("generation") != generation(conn):
        raise CurrentError("release generation differs from source store")
    return {**manifest, "valid": True, "path": str(path)}


def _inspect_relations(path, conn, *, bind_store, expected=None):
    """Measure actual files and source rows; no proof survives this inspection call."""
    sources, relations = {}, {}
    for table, schema in SCHEMAS.items():
        file = path / f"{table}.parquet"
        physical = _physical(file)
        recorded = expected["relations"][table] if expected is not None else None
        if recorded is not None and physical != {
            key: recorded.get(key) for key in ("sha256", "bytes")
        }:
            raise CurrentError(f"corrupt {table} Parquet")
        source = _parquet(file)
        actual = conn.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()
        if tuple((row[0], row[1]) for row in actual) != schema:
            raise CurrentError(f"invalid {table} schema")
        fingerprint = semantic_fingerprint(conn, f"SELECT {projection(table)} FROM {source}")
        if recorded is not None:
            if any(recorded.get(key) != value for key, value in fingerprint.items()):
                raise CurrentError(f"invalid {table} semantic digest")
            if expected.get("counts", {}).get(table) != fingerprint["rows"]:
                raise CurrentError(f"invalid {table} count")
        if bind_store and table != "outcomes":
            source_digest = semantic_fingerprint(conn, f"SELECT {projection(table)} FROM {table}")
            if source_digest != fingerprint:
                raise CurrentError(f"{table} export differs from source store")
        sources[table] = source
        relations[table] = {**fingerprint, **physical}
    if bind_store:
        _, source_outcomes = _validate_store(conn)
        if any(relations["outcomes"].get(key) != value for key, value in source_outcomes.items()):
            raise CurrentError("outcomes export differs from source store")
    else:
        _integrity(conn, sources)
    return relations


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
    previous = pointer.get("previous_release_id")
    if previous is not None and (
        not isinstance(previous, str)
        or not re.fullmatch(r"[0-9a-f]{32}", previous)
        or previous == release_id
    ):
        raise CurrentError(
            "invalid previous release identity; preserve this root and restore a verified backup"
        )
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
    result = (
        _verify_release_on_connection(path, connection, bind_store=same_generation)
        if connection is not None
        else verify_release(path, settings=settings)
    )
    result["published_generation"] = result["generation"]
    if connection is not None:
        result["store_generation"] = generation(connection)
    return result


def _export(conn, table, destination):
    conn.execute(
        f"COPY (SELECT {projection(table)} FROM {table}) TO ? "
        "(FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3)",
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
    conn.execute("BEGIN TRANSACTION")
    try:
        if conn.execute("SELECT 1 FROM sync_state WHERE key='seed_pending'").fetchone():
            raise CurrentError(
                "seed import incomplete; resume import-release with its frozen source"
            )
        if current.exists():
            pointer = _pointer(settings)
            try:
                verified = verify_current(settings, connection=conn)
            except (CurrentError, OSError, ValueError, duckdb.Error):
                fallback = pointer.get("previous_release_id")
                if fallback is not None:
                    try:
                        if not isinstance(fallback, str) or not re.fullmatch(
                            r"[0-9a-f]{32}", fallback
                        ):
                            raise CurrentError("invalid previous release identity")
                        verified = _verify_release_on_connection(
                            releases / fallback, conn, bind_store=False
                        )
                        if (
                            verified.get("release_id") != fallback
                            or type(verified.get("generation")) is not int
                            or not 0 <= verified["generation"] <= generation(conn)
                        ):
                            raise CurrentError("invalid previous release binding")
                    except (CurrentError, OSError, ValueError, duckdb.Error) as exc:
                        raise CurrentError(
                            "current and previous releases could not be verified; "
                            "preserve this root and restore a verified backup into a fresh root"
                        ) from exc
                    previous = fallback
            else:
                previous = pointer["release_id"]
                if verified["generation"] == generation(conn):
                    _cleanup(
                        settings, {pinned.get("release_id"), pinned.get("previous_release_id")}
                    )
                    _check_deadline(deadline)
                    conn.execute("COMMIT")
                    return {**verified, "noop": True}
        _cleanup(settings, {pinned.get("release_id"), pinned.get("previous_release_id")})
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
            with file.open("rb") as handle:
                os.fsync(handle.fileno())
        manifest["relations"] = _inspect_relations(stage, conn, bind_store=True)
        if any(
            manifest["counts"][table] != value["rows"]
            for table, value in manifest["relations"].items()
        ):
            raise CurrentError("publication counts differ from verified rows")
        manifest_bytes = json.dumps(manifest, sort_keys=True).encode()
        atomic_write_bytes(stage / "manifest.json", manifest_bytes)
        if read_regular_bytes(stage / "manifest.json", max_bytes=4 * 1024**2) != manifest_bytes:
            raise CurrentError("candidate manifest changed after verification")
        if {child.name for child in stage.iterdir()} != {
            "manifest.json",
            *(f"{table}.parquet" for table in SCHEMAS),
        }:
            raise CurrentError("candidate inventory changed after verification")
        for table, descriptor in manifest["relations"].items():
            if _physical(stage / f"{table}.parquet") != {
                key: descriptor[key] for key in ("sha256", "bytes")
            }:
                raise CurrentError(f"candidate {table} changed after verification")
        if manifest["generation"] != generation(conn):
            raise CurrentError("candidate source generation changed after verification")
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
