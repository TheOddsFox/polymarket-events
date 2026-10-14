"""Offline, inventoried recovery bundles for the current catalogue."""

import json
import os
import re
import shutil
import stat
import tempfile
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from .capture.writer import _fsync_dir, atomic_write_bytes, read_body, read_regular_bytes
from .current_config import CurrentSettings, admit, connect_store, safe_path, validate_root
from .current_contract import BACKUP_CONTRACT, CurrentError
from .current_publish import _physical, _pointer, verify_current, verify_release
from .current_rows import project_response
from .current_store import get_state, set_state, validate_store
from .current_sync import (
    _pending_manifest,
    _records,
    validate_discovery_pending,
    validate_fetch_pending,
)
from .limits import directory_bytes

_ID = re.compile(r"[0-9a-f]{32}\Z")
_BACKUP = re.compile(r"\d{8}-[0-9a-f]{32}\Z")


def _deadline(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise CurrentError("backup deadline exhausted")


def _relative(value):
    if not isinstance(value, str):
        raise CurrentError("invalid inventory path")
    path = Path(value)
    if path.is_absolute() or not path.parts or any(p in {"..", "."} for p in value.split("/")):
        raise CurrentError("unsafe inventory path")
    allowed = value in {
        "catalogue.json",
        "catalogue.duckdb",
        "config.toml",
        "published/current.json",
    }
    allowed |= (
        len(path.parts) == 3
        and path.parts[0] == "raw"
        and bool(_ID.fullmatch(path.parts[1]))
        and path.name in {"p000001.json.gz", "p000001.manifest.json"}
    )
    allowed |= (
        len(path.parts) == 4
        and path.parts[:2] == ("published", "releases")
        and bool(_ID.fullmatch(path.parts[2]))
        and path.name
        in {
            "manifest.json",
            "events.parquet",
            "markets.parquet",
            "event_markets.parquet",
            "outcomes.parquet",
        }
    )
    if not allowed:
        raise CurrentError("file outside backup allowlist")
    return path


def _pinned(conn):
    ids = set()

    def walk(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if key == "raw_id":
                    if not isinstance(item, str) or not _ID.fullmatch(item):
                        raise CurrentError("unsafe pending raw identity")
                    ids.add(item)
                else:
                    walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    for (value,) in conn.execute("SELECT value FROM sync_state").fetchall():
        walk(json.loads(value))
    return ids


def _validate_existing_pending(settings, conn):
    """Prove durable pending receipts can resume, without making a request."""
    discovery = get_state(conn, "discovery", {}).get("pending")
    fetch = get_state(conn, "fetch_pending")
    for work, is_discovery in ((discovery, True), (fetch, False)):
        if work is None:
            continue
        identifier = work.get("raw_id")
        if not isinstance(identifier, str) or not _ID.fullmatch(identifier):
            raise CurrentError("unsafe pending raw identity")
        try:
            if is_discovery:
                validate_discovery_pending(settings, conn, work)
                lane = work["lane"]
                expected = work["remaining"][0]
                wanted, native = None, False
            else:
                validate_fetch_pending(settings, conn, work)
                identity = work["identity"]
                lane, native = identity["lane"], identity["native"]
                expected = work["remaining"][0]
                wanted = (
                    [int(expected["endpoint"].rsplit("/", 1)[1])]
                    if native
                    else expected["params"]["id"]
                )
            directory = safe_path(settings.raw_dir / identifier)
            manifest = _pending_manifest(directory, settings.root)
            if manifest is None:
                continue
            if manifest["request"] != expected:
                raise CurrentError("pending receipt request mismatch")
            body = read_body(
                directory,
                manifest,
                trusted_root=settings.root,
                max_body_bytes=settings.limits.max_response_bytes,
            )
            if native and manifest["status"] == 404:
                records = []
            else:
                records = _records(
                    SimpleNamespace(status=manifest["status"], json=json.loads(body)),
                    lane,
                    wanted,
                    native=native,
                    closed=expected["params"].get("closed"),
                )
            if is_discovery and work["kind"] == "highwater" and len(records) > 1:
                raise CurrentError("pending highwater response exceeds limit")
            project_response(
                records,
                lane,
                received_at=manifest["received_at"],
                response_id=identifier,
                native_market_ids=wanted if native and lane == "markets" else (),
            )
        except (KeyError, IndexError, TypeError, ValueError, OSError, EOFError) as exc:
            raise CurrentError("invalid pending response receipt") from exc


def _files(settings, conn):
    _validate_existing_pending(settings, conn)
    result = {"catalogue.json", "catalogue.duckdb"}
    if safe_path(settings.root / "config.toml").exists():
        result.add("config.toml")
    if safe_path(settings.published_dir / "current.json").exists():
        verify_current(settings, connection=conn)
        pointer = _pointer(settings)
        result.add("published/current.json")
        for identifier in {pointer["release_id"], pointer.get("previous_release_id")} - {None}:
            if not isinstance(identifier, str) or not _ID.fullmatch(identifier):
                raise CurrentError("unsafe previous release identity")
            release = settings.published_dir / "releases" / identifier
            verify_release(release, settings=settings)
            result.update(str(p.relative_to(settings.root)) for p in release.iterdir())
    for identifier in _pinned(conn):
        directory = safe_path(settings.raw_dir / identifier)
        if directory.exists():
            result.update(str(p.relative_to(settings.root)) for p in directory.iterdir())
    for name in result:
        _relative(name)
    return sorted(result)


def _copy(source, destination, inventory, deadline):
    for name, expected in inventory.items():
        _deadline(deadline)
        source_path = safe_path(source / _relative(name))
        target = safe_path(destination / name)
        target.parent.mkdir(parents=True, exist_ok=True)
        with source_path.open("rb") as incoming, target.open("xb") as outgoing:
            shutil.copyfileobj(incoming, outgoing, 1024**2)
            outgoing.flush()
            os.fsync(outgoing.fileno())
        if _physical(target) != expected:
            raise CurrentError("backup copy changed or is corrupt")
        _fsync_dir(target.parent)


def _validate(settings, deadline):
    _deadline(deadline)
    with connect_store(settings, read_only=True) as conn:
        result = validate_store(conn)
        expected = _files(settings, conn)
        if (settings.published_dir / "current.json").exists():
            verify_current(settings, connection=conn)
    _deadline(deadline)
    return result, expected


def _inventory(path, deadline):
    safe_path(path)
    value = json.loads(read_regular_bytes(path / "backup.json", max_bytes=4 * 1024**2))
    if value.get("contract") != BACKUP_CONTRACT or not isinstance(value.get("files"), dict):
        raise CurrentError("unsupported backup contract")
    inventory = value["files"]
    if not {"catalogue.json", "catalogue.duckdb"}.issubset(inventory):
        raise CurrentError("incomplete backup")
    actual = set()
    for child in path.rglob("*"):
        safe_path(child)
        if child.is_file():
            actual.add(str(child.relative_to(path)))
    if actual != set(inventory) | {"backup.json"}:
        raise CurrentError("unexpected backup contents")
    for name, expected in inventory.items():
        _deadline(deadline)
        if _physical(path / _relative(name)) != expected:
            raise CurrentError("corrupt backup inventory")
    return value


def verify_backup(path, *, deadline=None):
    path = safe_path(Path(path))
    value = _inventory(path, deadline)
    size = sum(item["bytes"] for item in value["files"].values())
    limits = CurrentSettings(path).limits
    if size >= limits.max_temp_bytes:
        raise CurrentError("backup verification temporary allowance exhausted")
    if (
        shutil.disk_usage(Path(tempfile.gettempdir()).resolve()).free - limits.max_temp_bytes
        < limits.free_reserve_bytes
    ):
        raise CurrentError("backup verification free-space reserve exhausted")
    # A fresh private restore proves recovery without trusting ambient configuration.
    with tempfile.TemporaryDirectory(prefix="catalogue-verify-") as directory:
        root = Path(directory).resolve() / "root"
        root.mkdir()
        _copy(path, root, value["files"], deadline)
        result, expected = _validate(
            CurrentSettings(
                root, limits=replace(limits, max_temp_bytes=limits.max_temp_bytes - size)
            ),
            deadline,
        )
        if set(expected) != set(value["files"]):
            raise CurrentError("backup inventory does not match recovery state")
    return {**value, **result, "path": str(path)}


def create_backup(settings, *, deadline=None):
    validate_root(settings.root)
    _deadline(deadline)
    with connect_store(settings) as conn:
        validate_store(conn)
        files = _files(settings, conn)
        conn.execute("CHECKPOINT")
    inventory = {name: _physical(settings.root / name) for name in files}
    size = sum(item["bytes"] for item in inventory.values())
    if size > settings.limits.max_temp_bytes:
        raise CurrentError("backup verification temporary allowance exhausted")
    admit(settings, additional_bytes=2 * size + 65536)
    backups = safe_path(settings.backups_dir)
    backups.mkdir(exist_ok=True)
    identifier = datetime.now(UTC).strftime("%Y%m%d") + "-" + uuid4().hex
    stage = backups / (".staging-" + uuid4().hex)
    stage.mkdir()
    try:
        _copy(settings.root, stage, inventory, deadline)
        atomic_write_bytes(
            stage / "backup.json",
            json.dumps({"contract": BACKUP_CONTRACT, "files": inventory}, sort_keys=True).encode(),
        )
        verify_backup(stage, deadline=deadline)
        _deadline(deadline)
        _fsync_dir(stage)
        final = backups / identifier
        os.rename(stage, final)
        _fsync_dir(backups)
    except BaseException:
        shutil.rmtree(stage)
        raise
    result = {
        "valid": True,
        "path": str(final),
        "bytes": size,
        "created_at": datetime.now(UTC).isoformat(),
    }
    with connect_store(settings) as conn:
        set_state(conn, "last_backup", result)
    return result


def restore_backup(path, destination, *, deadline=None):
    path = safe_path(Path(path))
    verified = verify_backup(path, deadline=deadline)
    destination = safe_path(Path(destination))
    if destination.exists():
        raise CurrentError("restore destination must not exist")
    # Validate ancestor legacy/root guards without creating the requested destination.
    for ancestor in destination.parents:
        if any(
            (ancestor / marker).exists()
            for marker in ("catalogue.json", "ledger.sqlite", "warehouse", ".state/ledger.sqlite")
        ):
            raise CurrentError("restore destination is nested in a catalogue or legacy root")
    size = sum(item["bytes"] for item in verified["files"].values())
    existing = next(parent for parent in destination.parents if parent.exists())
    if (
        shutil.disk_usage(existing).free - size
        < CurrentSettings(destination).limits.free_reserve_bytes
    ):
        raise CurrentError("restore free-space reserve exhausted")
    destination.mkdir(parents=True)
    try:
        inventory = dict(verified["files"])
        stamp = inventory.pop("catalogue.json")
        _copy(path, destination, inventory, deadline)
        # All data was validated in the private restore; readiness is written last.
        _copy(path, destination, {"catalogue.json": stamp}, deadline)
        _fsync_dir(destination)
        _fsync_dir(destination.parent)
    except BaseException:
        shutil.rmtree(destination)
        raise
    return {"valid": True, "path": str(destination)}


def prune(settings, *, now=None, raw_only=False):
    validate_root(settings.root)
    now = time.time() if now is None else now
    if hasattr(now, "timestamp"):
        now = now.timestamp()
    with connect_store(settings, read_only=True) as conn:
        pinned = _pinned(conn)
    deleted = 0
    raw = safe_path(settings.raw_dir)
    candidates = []
    if raw.exists():
        for child in raw.iterdir():
            safe_path(child)
            if child.is_dir() and _ID.fullmatch(child.name):
                files = list(child.iterdir())
                if any(p.name not in {"p000001.json.gz", "p000001.manifest.json"} for p in files):
                    continue
                size = 0
                for file in files:
                    safe_path(file)
                    metadata = file.stat()
                    if not stat.S_ISREG(metadata.st_mode):
                        raise CurrentError("raw retention contains a special file")
                    size += metadata.st_size
                age = max((p.stat().st_mtime for p in files), default=child.stat().st_mtime)
                candidates.append((age, child, size))
    total = directory_bytes(raw)
    if (
        sum(size for _, child, size in candidates if child.name in pinned)
        > settings.limits.max_raw_bytes
    ):
        raise CurrentError("pinned raw exceeds allowance; resume pending work before pruning")
    for age, child, size in sorted(candidates):
        if child.name not in pinned and (
            now - age > settings.limits.raw_max_age_s or total > settings.limits.max_raw_bytes
        ):
            shutil.rmtree(child)
            total -= size
            deleted += 1
    if total > settings.limits.max_raw_bytes:
        raise CurrentError(
            "raw allowance exceeded by unowned files; inspect without deleting evidence"
        )
    if raw_only:
        admit(settings)
        return {"deleted": deleted, "raw_bytes": total}
    published = safe_path(settings.published_dir)
    keep = set()
    if (published / "current.json").exists():
        verify_current(settings)
        pointer = _pointer(settings)
        keep = {pointer["release_id"], pointer.get("previous_release_id")}
    releases = safe_path(published / "releases")
    if releases.exists():
        for child in releases.iterdir():
            safe_path(child)
            if child.is_dir() and _ID.fullmatch(child.name) and child.name not in keep:
                directory_bytes(child)
                shutil.rmtree(child)
                deleted += 1
    backups = safe_path(settings.backups_dir)
    for directory in (published, backups):
        if directory.exists():
            for child in directory.iterdir():
                safe_path(child)
                if (
                    child.is_dir()
                    and child.name.startswith(".staging-")
                    and _ID.fullmatch(child.name[9:])
                ):
                    directory_bytes(child)
                    shutil.rmtree(child)
                    deleted += 1
    verified = []
    if backups.exists():
        for child in backups.iterdir():
            safe_path(child)
            if child.is_dir() and _BACKUP.fullmatch(child.name):
                try:
                    verify_backup(child)
                except (CurrentError, ValueError, OSError):
                    continue
                verified.append(child)
        days = set()
        for child in sorted(verified, reverse=True):
            day = child.name[:8]
            if day not in days and len(days) < 7:
                days.add(day)
            else:
                directory_bytes(child)
                shutil.rmtree(child)
                deleted += 1
    admit(settings)
    return {"deleted": deleted, "raw_bytes": total}
