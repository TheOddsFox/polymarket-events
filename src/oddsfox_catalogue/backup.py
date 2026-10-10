"""Portable, finite backups and fresh-root restoration of operator evidence."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import uuid
from dataclasses import asdict, fields, replace
from datetime import datetime
from pathlib import Path, PurePosixPath

from oddsfox_catalogue.capture.writer import _fsync_dir, atomic_write_bytes, read_regular_bytes
from oddsfox_catalogue.config import Settings, _validate
from oddsfox_catalogue.ids import iso_utc, utc_now
from oddsfox_catalogue.limits import enforce_storage_limits, retained_bytes
from oddsfox_catalogue.runlock import current_git_sha
from oddsfox_catalogue.semantics import bounded_connection

MANIFEST = "backup.json"
BACKUP_VERSION = 2
MAX_MANIFEST_BYTES = 64 * 1024**2
MAX_MEMBERS = 1_000_000
MAX_RETAINED_BYTES = 64 * 1024**3
TREE_NAMES = ("raw", "metadata", "published", "dlt_pipelines")
FILE_NAMES = ("ledger.sqlite", "catalogue.duckdb")
CONFIG_SECTIONS = ("gamma", "load", "quality", "schedule", "capture")


def _safe_path(path: Path) -> Path:
    path = Path(os.path.abspath(path))
    for ancestor in (path, *path.parents):
        if ancestor.is_symlink():
            raise ValueError("backup path contains a symlink")
    return path


def _inventory(root: Path, *, max_members: int = MAX_MEMBERS):
    root = _safe_path(root)
    if not root.is_dir():
        raise ValueError("backup root is not a directory")
    files, directories, pending = {}, [], [root]
    while pending:
        directory = pending.pop()
        for path in directory.iterdir():
            relative = path.relative_to(root).as_posix()
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                directories.append(relative)
                pending.append(path)
            elif stat.S_ISREG(info.st_mode):
                files[relative] = info.st_size
            else:
                raise ValueError("backup contains a symlink or special file")
            if len(files) + len(directories) > max_members:
                raise ValueError("backup member count exceeds limit")
    return files, sorted(directories)


def _stream_file(
    path: Path, *, destination: Path | None = None, max_bytes: int = MAX_RETAINED_BYTES
):
    path = _safe_path(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    digest, size = hashlib.sha256(), 0
    with os.fdopen(fd, "rb") as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise ValueError("backup member is not a regular file")
        target = None
        if destination is not None:
            _safe_path(destination)
            target = destination.open("xb")
        try:
            while chunk := source.read(1024**2):
                digest.update(chunk)
                size += len(chunk)
                if size > max_bytes:
                    raise ValueError("backup member exceeds byte limit")
                if target is not None:
                    target.write(chunk)
            if target is not None:
                target.flush()
                os.fsync(target.fileno())
        finally:
            if target is not None:
                target.close()
    return {"bytes": size, "sha256": digest.hexdigest()}


def _relative(value: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise ValueError("unsafe backup inventory path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {".", ".."} for part in path.parts):
        raise ValueError("unsafe backup inventory path")
    if path.as_posix() != value:
        raise ValueError("noncanonical backup inventory path")
    if path.parts[0] not in (*TREE_NAMES, *FILE_NAMES):
        raise ValueError("unexpected backup inventory path")
    if path.parts[0] in FILE_NAMES and len(path.parts) != 1:
        raise ValueError("invalid backup file location")
    return value


def _portable_config(settings: Settings):
    return {name: asdict(getattr(settings, name)) for name in CONFIG_SECTIONS}


def _config_settings(config) -> Settings:
    if not isinstance(config, dict) or set(config) != set(CONFIG_SECTIONS):
        raise ValueError("invalid backup settings")
    settings = Settings(Path("/"))
    sections = {}
    for name in CONFIG_SECTIONS:
        values, default = config[name], getattr(settings, name)
        if not isinstance(values, dict) or set(values) != {field.name for field in fields(default)}:
            raise ValueError("invalid backup settings fields")
        for key, value in values.items():
            expected = getattr(default, key)
            if type(value) is not type(expected) and not (
                type(expected) is float and type(value) is int
            ):
                raise ValueError("invalid backup settings types")
        sections[name] = type(default)(**values)
    settings = replace(settings, **sections)
    _validate(settings)
    return settings


def _config_bytes(config) -> bytes:
    _config_settings(config)
    lines = []
    for section in CONFIG_SECTIONS:
        lines.append(f"[{section}]")
        for key, value in config[section].items():
            literal = json.dumps(value, allow_nan=False)
            lines.append(f"{key} = {literal}")
        lines.append("")
    return ("\n".join(lines) + "\n").encode()


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate backup JSON field")
        result[key] = value
    return result


def _validated_manifest(backup_dir: Path, *, max_bytes: int = MAX_RETAINED_BYTES):
    backup_dir = _safe_path(backup_dir)
    manifest = json.loads(
        read_regular_bytes(
            backup_dir / MANIFEST, max_bytes=MAX_MANIFEST_BYTES, trusted_root=backup_dir
        ),
        object_pairs_hook=_unique_object,
    )
    if (
        not isinstance(manifest, dict)
        or type(manifest.get("backup_version")) is not int
        or manifest["backup_version"] != BACKUP_VERSION
    ):
        raise ValueError("unsupported backup version")
    if set(manifest) != {
        "backup_version",
        "created_at",
        "git_sha",
        "settings",
        "directories",
        "files",
    }:
        raise ValueError("unexpected backup manifest fields")
    files, directories = manifest.get("files"), manifest.get("directories")
    if (
        not isinstance(files, dict)
        or not isinstance(directories, list)
        or len(files) + len(directories) > MAX_MEMBERS
    ):
        raise ValueError("invalid backup inventory")
    directories = [_relative(name) for name in directories]
    if any(name.split("/")[0] in FILE_NAMES for name in directories):
        raise ValueError("invalid backup directory type")
    if len(directories) != len(set(directories)) or not set(TREE_NAMES) <= set(directories):
        raise ValueError("invalid backup directory inventory")
    if set(files) & set(directories):
        raise ValueError("conflicting backup inventory types")
    total = 0
    for name, entry in files.items():
        _relative(name)
        if (
            not isinstance(entry, dict)
            or set(entry) != {"sha256", "bytes"}
            or type(entry["bytes"]) is not int
            or entry["bytes"] < 0
            or not isinstance(entry["sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"])
        ):
            raise ValueError("invalid backup inventory entry")
        total += entry["bytes"]
        if total > max_bytes:
            raise ValueError("backup retained bytes exceed limit")
    config = _config_bytes(manifest.get("settings"))
    max_bytes = min(max_bytes, _config_settings(manifest["settings"]).capture.max_retained_bytes)
    if total + len(config) + (backup_dir / MANIFEST).stat().st_size > max_bytes:
        raise ValueError("backup retained bytes exceed limit")
    actual, actual_dirs = _inventory(backup_dir)
    expected = set(files) | {MANIFEST}
    problems = [f"missing {name}" for name in sorted(expected - set(actual))]
    problems += [f"unlisted file {name}" for name in sorted(set(actual) - expected)]
    problems += [
        f"missing directory {name}" for name in sorted(set(directories) - set(actual_dirs))
    ]
    problems += [
        f"unlisted directory {name}" for name in sorted(set(actual_dirs) - set(directories))
    ]
    for name, entry in files.items():
        if name not in actual:
            continue
        if actual[name] != entry["bytes"]:
            problems.append(f"size mismatch {name}")
            continue
        if _stream_file(backup_dir / name, max_bytes=entry["bytes"])["sha256"] != entry["sha256"]:
            problems.append(f"checksum mismatch {name}")
    if problems:
        raise ValueError("\n".join(problems))
    return manifest, config


def _snapshot_ledger(source: Path, dest: Path) -> None:
    _safe_path(source)
    src = sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)
    try:
        dst = sqlite3.connect(dest)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def create_backup(
    settings: Settings, *, dest_root: Path | None = None, now: datetime | None = None
) -> Path:
    """Write an immutable backup. The caller holds the operator root lock."""
    enforce_storage_limits(settings)
    original_bytes = retained_bytes(settings)
    when = now or utc_now()
    sources = {
        "raw": settings.raw_dir,
        "metadata": settings.data_dir / "metadata",
        "published": settings.published_dir,
        "dlt_pipelines": settings.dlt_pipelines_dir,
    }
    root = _safe_path(settings.root)
    for source in (*sources.values(), settings.ledger_path, settings.warehouse_path):
        if not _safe_path(source).is_relative_to(root):
            raise ValueError("portable backup requires operator paths inside its root")
    from oddsfox_catalogue.resources import dbt_project_dir

    if (
        settings.dbt_project_dir != dbt_project_dir()
        or settings.dbt_profiles_dir != dbt_project_dir()
    ):
        raise ValueError("portable backup requires the packaged dbt resources")
    source_files, source_dirs, tree_inventories = {}, set(TREE_NAMES), {}
    for name, source in sources.items():
        if source.exists():
            found, directories = _inventory(source)
            tree_inventories[name] = (found, directories)
            source_files.update({f"{name}/{key}": source / key for key in found})
            source_dirs.update(f"{name}/{key}" for key in directories)
    if len(source_files) + len(source_dirs) + 2 > MAX_MEMBERS:
        raise ValueError("backup member count exceeds limit")
    for name in (*source_files, *source_dirs):
        _relative(name)
    estimate = sum(path.stat().st_size for path in source_files.values())
    estimate += sum(
        path.stat().st_size
        for path in (settings.ledger_path, settings.warehouse_path)
        if path.exists()
    )
    estimate += sum(
        path.stat().st_size
        for path in (
            settings.ledger_path.with_name(settings.ledger_path.name + "-wal"),
            settings.warehouse_path.with_name(settings.warehouse_path.name + ".wal"),
        )
        if path.exists()
    )
    estimate += min(MAX_MANIFEST_BYTES, (len(source_files) + len(source_dirs) + 2) * 300) + 65536
    enforce_storage_limits(settings, additional_bytes=estimate)
    parent = _safe_path(dest_root or settings.data_dir / "backups")
    if any(parent.is_relative_to(source) for source in sources.values()):
        raise ValueError("backup destination cannot be inside source evidence")
    parent.mkdir(parents=True, exist_ok=True)
    dest = parent / f"{when.strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex}"
    dest.mkdir()
    for directory in sorted(source_dirs):
        (dest / directory).mkdir(parents=True, exist_ok=True)
    files = {}
    if settings.ledger_path.exists():
        _snapshot_ledger(settings.ledger_path, dest / "ledger.sqlite")
        files["ledger.sqlite"] = _stream_file(dest / "ledger.sqlite")
    if settings.warehouse_path.exists():
        with bounded_connection(settings, read_only=False) as connection:
            connection.execute("CHECKPOINT")
        files["catalogue.duckdb"] = _stream_file(
            settings.warehouse_path,
            destination=dest / "catalogue.duckdb",
            max_bytes=settings.warehouse_path.stat().st_size,
        )
    copied_bytes = sum(entry["bytes"] for entry in files.values())
    for name, source in sorted(source_files.items()):
        files[name] = _stream_file(source, destination=dest / name, max_bytes=source.stat().st_size)
        copied_bytes += files[name]["bytes"]
        if original_bytes + copied_bytes > settings.capture.max_retained_bytes:
            raise ValueError("backup exceeds retained storage allowance")
    for name, source in sources.items():
        actual = _inventory(source) if source.exists() else None
        if actual != tree_inventories.get(name):
            raise ValueError("backup source inventory changed during copying")
    for name, source in source_files.items():
        if _stream_file(source, max_bytes=files[name]["bytes"]) != files[name]:
            raise ValueError("backup source changed during copying")
    manifest = {
        "backup_version": BACKUP_VERSION,
        "created_at": iso_utc(when),
        "git_sha": current_git_sha(settings.root),
        "settings": _portable_config(settings),
        "directories": sorted(source_dirs),
        "files": files,
    }
    encoded = (json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    if len(encoded) > MAX_MANIFEST_BYTES or len(files) + len(source_dirs) > MAX_MEMBERS:
        raise ValueError("backup inventory exceeds limits")
    enforce_storage_limits(settings, additional_bytes=len(encoded))
    for directory in sorted(source_dirs, key=lambda value: value.count("/"), reverse=True):
        _fsync_dir(dest / directory)
    atomic_write_bytes(dest / MANIFEST, encoded)
    _validated_manifest(dest, max_bytes=settings.capture.max_retained_bytes)
    return dest


def verify_backup(backup_dir: Path, *, max_bytes: int = MAX_RETAINED_BYTES) -> list[str]:
    """Validate every path, type, size and checksum without copying anything."""
    try:
        _validated_manifest(backup_dir, max_bytes=max_bytes)
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError, RecursionError) as exc:
        return str(exc).splitlines()
    return []


def restore_backup(backup_dir: Path, destination: Path) -> Path:
    """Verify first, then copy into an exclusively created fresh operator root."""
    manifest, config = _validated_manifest(backup_dir)
    destination = _safe_path(destination)
    if destination.is_relative_to(_safe_path(backup_dir)):
        raise ValueError("restoration destination cannot be inside its backup")
    if destination.exists():
        raise FileExistsError("backup restoration requires a fresh destination")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.mkdir()  # Exclusive creation protects an existing root, including an empty one.
    mapping = {
        "raw": "data/raw",
        "metadata": "data/metadata",
        "published": "data/published",
        "dlt_pipelines": ".state/dlt",
        "ledger.sqlite": ".state/ledger.sqlite",
        "catalogue.duckdb": "data/warehouse/catalogue.duckdb",
    }
    for directory in manifest["directories"]:
        first, *rest = directory.split("/")
        (destination / mapping[first] / "/".join(rest)).mkdir(parents=True, exist_ok=True)
    for name, expected in manifest["files"].items():
        first, *rest = name.split("/")
        target = destination / mapping[first] / "/".join(rest)
        target.parent.mkdir(parents=True, exist_ok=True)
        if (
            _stream_file(backup_dir / name, destination=target, max_bytes=expected["bytes"])
            != expected
        ):
            raise ValueError("backup changed during restoration; partial destination retained")
    atomic_write_bytes(destination / "config" / "catalogue.toml", config)
    # Restore uses the captured quality/resource settings and default portable paths.
    from oddsfox_catalogue.config import load_settings

    restored = load_settings(root=destination, env={})
    if retained_bytes(restored) > restored.capture.max_retained_bytes:
        raise ValueError("restored root exceeds retained storage allowance")
    _, directories = _inventory(destination)
    for directory in sorted(directories, key=lambda value: value.count("/"), reverse=True):
        _fsync_dir(destination / directory)
    _fsync_dir(destination)
    return destination
