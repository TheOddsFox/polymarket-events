"""Verified immutable catalogue releases and a confined atomic current pointer.

Interrupted candidates and every prior release are retained. Only a fully
verified, certified candidate may replace the previous pointer.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

import duckdb

from oddsfox_catalogue.capture.writer import read_regular_bytes
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.contract import (
    CONTRACT,
    NORMALIZATION_REVISION,
    projection_queries,
    projection_schemas,
)
from oddsfox_catalogue.faults import fault_point
from oddsfox_catalogue.fingerprints import SEMANTIC_DIGEST_REVISION, semantic_fingerprint
from oddsfox_catalogue.ids import iso_utc, parse_batch_id, utc_now
from oddsfox_catalogue.inventory import capture_inventory
from oddsfox_catalogue.limits import enforce_storage_limits
from oddsfox_catalogue.semantics import bounded_connection, canonical_json_chunks, json_descriptor
from oddsfox_catalogue.warehouse import BaselineMissing

PROJECTION_TABLE_QUERIES = projection_queries()
CURRENT_POINTER = "current.json"
JSON_LIMIT = 128 * 1024**2
MANIFEST_LIMIT = 1024**2
RELEASE_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z(?:-[1-9][0-9]*)?\Z")
HASH = re.compile(r"[a-f0-9]{64}\Z")
UTC_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|\+00:00)\Z"
)


class PublishBlocked(RuntimeError):
    """Publication or verification failed a concrete evidence check."""


@dataclass
class ReleaseInfo:
    release_id: str
    path: Path
    tables: dict[str, dict[str, Any]] = field(default_factory=dict)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise ValueError("release member is not a regular file")
        for chunk in iter(lambda: handle.read(1024**2), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_json(path: Path, value, *, max_bytes=JSON_LIMIT) -> dict[str, Any]:
    # Admission occurs before writing; encoder traversal is bounded and streamed.
    descriptor = json_descriptor(value)
    if descriptor["bytes"] > max_bytes:
        raise PublishBlocked("release JSON exceeds its finite size limit")
    with path.open("xb") as handle:
        for chunk in canonical_json_chunks(value, max_bytes=max_bytes):
            handle.write(chunk)
        handle.flush()
        os.fsync(handle.fileno())
    return {"file": path.name, **descriptor}


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _utc_timestamp(value):
    if not isinstance(value, str) or not UTC_TIMESTAMP.fullmatch(value):
        raise ValueError("release evidence has an invalid UTC timestamp")
    return datetime.fromisoformat(value)


def _read_json(path: Path, root: Path, limit: int):
    data = read_regular_bytes(path, trusted_root=root, max_bytes=limit)

    def reject_constant(value):
        raise ValueError("non-finite JSON number")

    return json.loads(data, object_pairs_hook=_unique_pairs, parse_constant=reject_constant), data


def fingerprint_parquet(connection: duckdb.DuckDBPyConnection, path: Path) -> dict[str, Any]:
    return semantic_fingerprint(connection, "SELECT * FROM read_parquet(?)", [str(path)])


def release_id_for(now: datetime, existing: set[str]) -> str:
    base = now.strftime("%Y%m%dT%H%M%SZ")
    candidate, suffix = base, 1
    while candidate in existing:
        suffix += 1
        candidate = f"{base}-{suffix}"
    return candidate


def _descriptor(path: Path, declared, root: Path, *, max_bytes=None) -> None:
    if not isinstance(declared, dict) or declared.get("file") != path.name:
        raise ValueError("release output descriptor has an unexpected filename")
    size, checksum = declared.get("bytes"), declared.get("sha256")
    if isinstance(size, bool) or not isinstance(size, int) or size < 0:
        raise ValueError("release output has invalid byte accounting")
    if not isinstance(checksum, str) or HASH.fullmatch(checksum) is None:
        raise ValueError("release output has an invalid checksum")
    # Confinement and regular-file checks precede opening any output, including Parquet.
    if any(p.is_symlink() for p in (path, *path.parents)) or not path.is_file():
        raise ValueError("release output is not a confined regular file")
    path.relative_to(root)
    if path.stat().st_size != size or (max_bytes is not None and size > max_bytes):
        raise ValueError("release output size differs from its descriptor")
    if _sha256_file(path) != checksum:
        raise ValueError("release output checksum mismatch")


def _validate_coverage(coverage, inventory, batch_ids) -> None:
    if not isinstance(inventory, list) or not inventory:
        raise ValueError("release capture inventory is missing")
    paths = []
    for entry in inventory:
        if not isinstance(entry, dict) or set(entry) != {"path", "bytes", "sha256"}:
            raise ValueError("invalid capture inventory entry")
        relative = entry["path"]
        if not isinstance(relative, str):
            raise ValueError("invalid capture inventory path")
        parts = PurePosixPath(relative)
        if (
            parts.is_absolute()
            or relative != parts.as_posix()
            or any(p in {"", ".", ".."} for p in relative.split("/"))
            or "\\" in relative
        ):
            raise ValueError("unsafe capture inventory path")
        if len(parts.parts) not in {3, 4} or parts.parts[1] not in batch_ids:
            raise ValueError("capture inventory has an undeclared batch")
        if (
            isinstance(entry["bytes"], bool)
            or not isinstance(entry["bytes"], int)
            or entry["bytes"] < 0
        ):
            raise ValueError("invalid capture inventory byte accounting")
        if not isinstance(entry["sha256"], str) or not HASH.fullmatch(entry["sha256"]):
            raise ValueError("invalid capture inventory checksum")
        paths.append(relative)
    if paths != sorted(set(paths)):
        raise ValueError("capture inventory paths must be distinct and ordered")
    if not isinstance(coverage, dict) or coverage.get("declared_scans_complete") is not True:
        raise ValueError("release does not account for complete declared scans")
    if coverage.get("source_catalogue_complete") is not False or not coverage.get(
        "discovery_limits"
    ):
        raise ValueError("release omits its source-discovery limitations")
    batches = coverage.get("batches")
    if (
        not isinstance(batches, list)
        or [b.get("batch_id") for b in batches if isinstance(b, dict)] != batch_ids
    ):
        raise ValueError("coverage and release batch inventories differ")
    for batch in batches:
        scope = batch.get("scope", {})
        if (
            not isinstance(scope, dict)
            or type(scope.get("revision")) is not int
            or scope.get("revision") != 2
            or scope.get("sealed") is not True
        ):
            raise ValueError("coverage has an unsupported or unsealed scope")
        _, mode = parse_batch_id(batch["batch_id"])
        if batch.get("mode") != mode:
            raise ValueError("coverage batch mode differs from its identity")
    units = coverage.get("units")
    if not isinstance(units, list):
        raise ValueError("release coverage units are missing")
    seen = set()
    page_paths = set()
    for unit in units:
        if not isinstance(unit, dict) or unit.get("batch_id") not in batch_ids:
            raise ValueError("coverage has an undeclared batch")
        _utc_timestamp(unit.get("received_at"))
        if not isinstance(unit.get("params"), dict) or not isinstance(unit.get("endpoint"), str):
            raise ValueError("coverage has invalid request provenance")
        key = unit.get("page_id")
        if not isinstance(key, str) or key in seen:
            raise ValueError("coverage unit identities must be distinct")
        seen.add(key)
        count = unit.get("records")
        status = unit.get("status")
        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or count < 0
            or status not in {"absent", "success_empty", "success"}
        ):
            raise ValueError("invalid coverage unit accounting")
        if (status in {"absent", "success_empty"} and count != 0) or (
            status == "success" and count == 0
        ):
            raise ValueError("coverage empty and nonempty accounting disagree")
        match = re.fullmatch(r"(.+)\.p([0-9]{6,})", key)
        if match is None or not match[1].startswith(unit["batch_id"] + "."):
            raise ValueError("invalid coverage page locator")
        stamp, _ = parse_batch_id(unit["batch_id"])
        prefix = f"{stamp.date().isoformat()}/{unit['batch_id']}/{match[1]}/p{match[2]}"
        page_paths.update({prefix + ".manifest.json", prefix + ".json.gz"})
    recorded_pages = {p for p in paths if PurePosixPath(p).name.startswith("p")}
    if recorded_pages != page_paths:
        raise ValueError("coverage unit locators differ from the capture inventory")


def verify_release(settings: Settings, release_path: Path, manifest_sha256: str | None = None):
    """Verify only immutable release evidence; no warehouse certification is required."""
    try:
        releases = settings.published_dir / "releases"
        path = Path(release_path)
        if not path.is_absolute():
            path = releases / path
        if (
            path.parent != releases
            or any(p.is_symlink() for p in (path, *path.parents))
            or not path.is_dir()
        ):
            raise ValueError("release path is outside its confined releases root")
        manifest, raw = _read_json(path / "release.json", releases, MANIFEST_LIMIT)
        if manifest_sha256 is not None and (
            not isinstance(manifest_sha256, str)
            or not HASH.fullmatch(manifest_sha256)
            or hashlib.sha256(raw).hexdigest() != manifest_sha256
        ):
            raise ValueError("release manifest checksum mismatch")
        if not isinstance(manifest, dict):
            raise ValueError("invalid release manifest")
        if set(manifest) != {
            "release_id",
            "created_at",
            "git_sha",
            "contract",
            "normalization_revision",
            "projection_version",
            "batch_ids",
            "tables",
            "capture_inventory",
            "coverage",
            "build",
        }:
            raise ValueError("release manifest has unexpected or missing fields")
        build = manifest["build"]
        if (
            not isinstance(build, dict)
            or set(build) != {"model_revision", "quality", "effective_vars", "certification_sha256"}
            or not isinstance(build["quality"], dict)
            or not isinstance(build["effective_vars"], dict)
            or not isinstance(build["model_revision"], str)
            or not HASH.fullmatch(build["model_revision"])
            or not isinstance(build["certification_sha256"], str)
            or not HASH.fullmatch(build["certification_sha256"])
        ):
            raise ValueError("release build provenance is invalid")
        release_id = manifest.get("release_id")
        if (
            not isinstance(release_id, str)
            or not RELEASE_ID.fullmatch(release_id)
            or path.name not in {release_id, f".staging-{release_id}"}
        ):
            raise ValueError("release path does not identify its declared release")
        created = _utc_timestamp(manifest["created_at"])
        if (
            created.utcoffset() is None
            or created.utcoffset().total_seconds() != 0
            or created.strftime("%Y%m%dT%H%M%SZ") != release_id.split("-")[0]
        ):
            raise ValueError("release timestamp differs from its declared identity")
        if manifest["git_sha"] is not None and not isinstance(manifest["git_sha"], str):
            raise ValueError("release Git provenance has an invalid type")
        if (
            manifest.get("contract") != CONTRACT
            or manifest.get("normalization_revision") != NORMALIZATION_REVISION
            or manifest.get("projection_version") != "v2"
        ):
            raise ValueError("unsupported catalogue release version")
        tables = manifest.get("tables")
        if not isinstance(tables, dict) or set(tables) != set(PROJECTION_TABLE_QUERIES):
            raise ValueError("release has missing or unexpected public relations")
        expected = {
            "release.json",
            "coverage.json",
            "capture-inventory.json",
            *(name + ".parquet" for name in tables),
        }
        if {p.name for p in path.iterdir()} != expected:
            raise ValueError("release file inventory has missing or unexpected members")
        for name, entry in tables.items():
            _descriptor(path / f"{name}.parquet", entry, releases)
            if (
                entry.get("schema") != projection_schemas()[name]
                or entry.get("semantic_digest_revision") != SEMANTIC_DIGEST_REVISION
            ):
                raise ValueError("release relation schema or digest version is unsupported")
            if (
                set(entry)
                != {
                    "file",
                    "bytes",
                    "sha256",
                    "rows",
                    "schema",
                    "semantic_sha256",
                    "semantic_digest_revision",
                }
                or isinstance(entry.get("rows"), bool)
                or not isinstance(entry.get("rows"), int)
                or entry["rows"] < 0
                or not isinstance(entry.get("semantic_sha256"), str)
                or not HASH.fullmatch(entry["semantic_sha256"])
            ):
                raise ValueError("release has invalid relation accounting")
        _descriptor(
            path / "coverage.json", manifest.get("coverage"), releases, max_bytes=JSON_LIMIT
        )
        if any(
            set(manifest[key]) != {"file", "bytes", "sha256"}
            for key in ("coverage", "capture_inventory")
        ):
            raise ValueError("release JSON descriptor has unexpected fields")
        _descriptor(
            path / "capture-inventory.json",
            manifest.get("capture_inventory"),
            releases,
            max_bytes=JSON_LIMIT,
        )
        coverage, _ = _read_json(path / "coverage.json", releases, JSON_LIMIT)
        inventory, _ = _read_json(path / "capture-inventory.json", releases, JSON_LIMIT)
        batch_ids = manifest.get("batch_ids")
        if (
            not isinstance(batch_ids, list)
            or not batch_ids
            or any(not isinstance(b, str) for b in batch_ids)
            or batch_ids != sorted(set(batch_ids))
        ):
            raise ValueError("invalid release batch inventory")
        _validate_coverage(coverage, inventory, batch_ids)
        with bounded_connection(settings, Path(":memory:"), read_only=False) as connection:
            for name, entry in tables.items():
                actual = fingerprint_parquet(connection, path / f"{name}.parquet")
                if any(actual[key] != entry.get(key) for key in actual):
                    raise ValueError("release relation semantic digest mismatch")
        return manifest
    except PublishBlocked:
        raise
    except (ValueError, OSError, KeyError, TypeError, RecursionError, duckdb.Error) as exc:
        raise PublishBlocked("immutable release verification failed") from exc


def current_release(settings: Settings) -> dict[str, Any] | None:
    """Read a confined pointer only after verifying its manifest and every output."""
    pointer_path = settings.published_dir / CURRENT_POINTER
    if any(p.is_symlink() for p in (settings.published_dir, *settings.published_dir.parents)):
        raise PublishBlocked("current release root contains a symlink")
    if not pointer_path.exists() and not pointer_path.is_symlink():
        return None
    try:
        pointer, _ = _read_json(pointer_path, settings.published_dir, MANIFEST_LIMIT)
        if not isinstance(pointer, dict) or set(pointer) != {
            "release_id",
            "path",
            "published_at",
            "manifest_sha256",
        }:
            raise ValueError("invalid current pointer")
        release_id = pointer["release_id"]
        if (
            not isinstance(release_id, str)
            or not RELEASE_ID.fullmatch(release_id)
            or pointer["path"] != f"releases/{release_id}"
        ):
            raise ValueError("unsafe current release pointer")
        manifest = verify_release(
            settings, settings.published_dir / pointer["path"], pointer["manifest_sha256"]
        )
        if manifest["release_id"] != release_id:
            raise ValueError("current pointer and release differ")
        if pointer["published_at"] != manifest["created_at"]:
            raise ValueError("current pointer and release timestamps differ")
        return pointer
    except (ValueError, OSError, TypeError, KeyError) as exc:
        raise PublishBlocked("current release pointer is invalid") from exc


def publish_release(
    settings: Settings, *, now: datetime | None = None, git_sha: str | None = None
) -> ReleaseInfo:
    """Caller holds the root writer lock; certify and verify before pointer promotion."""
    from oddsfox_catalogue.certification import BuildInvalid, assert_build_valid

    if not settings.warehouse_path.exists():
        raise BaselineMissing("no warehouse; run capture, load and a complete build first")
    try:
        receipt = assert_build_valid(settings)
        current_release(settings)
    except BuildInvalid as exc:
        raise PublishBlocked("warehouse build certification is invalid") from exc
    when = now or utc_now()
    published, releases = settings.published_dir, settings.published_dir / "releases"
    releases.mkdir(parents=True, exist_ok=True)
    existing = {p.name.removeprefix(".staging-") for p in releases.iterdir()}
    release_id = release_id_for(when, existing)
    staging = releases / f".staging-{release_id}"
    staging.mkdir()
    manifest = _export_release(settings, staging, release_id, when, git_sha, receipt)
    verify_release(settings, staging)
    try:
        if assert_build_valid(settings)["binding"] != receipt["binding"]:
            raise PublishBlocked("warehouse changed during candidate export")
    except BuildInvalid as exc:
        raise PublishBlocked("warehouse changed during candidate export") from exc
    _fsync_dir(staging)
    final = releases / release_id
    os.rename(staging, final)
    _fsync_dir(releases)
    fault_point("mid_publish")
    pointer = {
        "release_id": release_id,
        "path": f"releases/{release_id}",
        "published_at": iso_utc(when),
        "manifest_sha256": _sha256_file(final / "release.json"),
    }
    temporary = published / f"{CURRENT_POINTER}.{release_id}.tmp"
    enforce_storage_limits(settings, additional_bytes=json_descriptor(pointer)["bytes"])
    _write_json(temporary, pointer, max_bytes=MANIFEST_LIMIT)
    enforce_storage_limits(settings)
    os.replace(temporary, published / CURRENT_POINTER)
    _fsync_dir(published)
    return ReleaseInfo(release_id=release_id, path=final, tables=manifest["tables"])


def _export_release(
    settings: Settings, staging: Path, release_id: str, when: datetime, git_sha: str | None, receipt
) -> dict[str, Any]:
    tables = {}
    with bounded_connection(settings) as connection:
        for name, query in PROJECTION_TABLE_QUERIES.items():
            # Bound admission using the exact JSON row bytes, plus conservative
            # pinned-writer overhead for Parquet headers, dictionaries and pages.
            size = connection.execute(
                f"SELECT coalesce(sum(octet_length(encode(to_json(r)))), 0) FROM ({query}) r"
            ).fetchone()[0]
            enforce_storage_limits(settings, additional_bytes=8 * 1024**2 + 4 * int(size))
            target = staging / f"{name}.parquet"
            literal = str(target).replace("'", "''")
            connection.execute(f"COPY ({query}) TO '{literal}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            with target.open("rb") as handle:
                os.fsync(handle.fileno())
            tables[name] = {
                "file": target.name,
                "bytes": target.stat().st_size,
                "sha256": _sha256_file(target),
                **fingerprint_parquet(connection, target),
            }
            expected = receipt["binding"]["published"][name]
            if any(tables[name].get(key) != value for key, value in expected.items()):
                raise PublishBlocked("exported relation differs from its certified projection")
            enforce_storage_limits(settings)
        registered = connection.execute(
            "SELECT batch_id, page_count FROM bronze.batch_registry ORDER BY batch_id"
        ).fetchall()
    try:
        inventory = capture_inventory(settings, [row[0] for row in registered], dict(registered))
    except (ValueError, OSError, KeyError) as exc:
        raise PublishBlocked("capture inventory cannot be verified") from exc
    inventory_descriptor = json_descriptor(inventory["files"])
    coverage_descriptor = json_descriptor(inventory["coverage"])
    capture = receipt["binding"]["capture"]
    if (
        capture["batch_ids"] != [row[0] for row in registered]
        or capture["inventory"] != inventory_descriptor
        or capture["coverage"] != coverage_descriptor
    ):
        raise PublishBlocked("capture inventory differs from the certified build")
    enforce_storage_limits(
        settings,
        additional_bytes=inventory_descriptor["bytes"]
        + coverage_descriptor["bytes"]
        + MANIFEST_LIMIT,
    )
    manifest = {
        "release_id": release_id,
        "created_at": iso_utc(when),
        "git_sha": git_sha,
        "contract": CONTRACT,
        "normalization_revision": NORMALIZATION_REVISION,
        "projection_version": "v2",
        "batch_ids": [row[0] for row in registered],
        "tables": tables,
        "build": {
            "model_revision": receipt["binding"]["model_revision"],
            "quality": receipt["binding"]["quality"],
            "effective_vars": receipt["binding"]["effective_vars"],
            "certification_sha256": json_descriptor(receipt)["sha256"],
        },
        "capture_inventory": _write_json(staging / "capture-inventory.json", inventory["files"]),
        "coverage": _write_json(staging / "coverage.json", inventory["coverage"]),
    }
    _write_json(staging / "release.json", manifest, max_bytes=MANIFEST_LIMIT)
    return manifest
