"""Offline, inventoried recovery bundles for the current catalogue."""

import gzip
import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import time
import zlib
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from .capture.writer import _fsync_dir, atomic_write_bytes, read_body, read_regular_bytes
from .current_config import CurrentSettings, admit, connect_store, safe_path, validate_root
from .current_contract import BACKUP_CONTRACT, CurrentError
from .current_publish import (
    _physical,
    _pointer,
    _verify_release_on_connection,
    verify_current,
)
from .current_rows import project_response
from .current_store import get_state, set_state, validate_store
from .current_sync import (
    _pending_manifest,
    _records,
    validate_discovery_pending,
    validate_fetch_pending,
)
from .legacy_release import _unique_pairs
from .limits import directory_bytes

_ID = re.compile(r"[0-9a-f]{32}\Z")
_BACKUP = re.compile(r"\d{8}-[0-9a-f]{32}\Z")


def _deadline(deadline):
    if deadline is not None and time.monotonic() >= deadline:
        raise CurrentError("backup deadline exhausted")


def _physical_checked(path, deadline):
    safe_path(path)
    if not stat.S_ISREG(path.stat().st_mode):
        raise CurrentError("backup contains a special file")
    digest, size = hashlib.sha256(), 0
    with path.open("rb") as handle:
        while block := handle.read(1024**2):
            _deadline(deadline)
            digest.update(block)
            size += len(block)
    _deadline(deadline)
    return {"bytes": size, "sha256": digest.hexdigest()}


def _verification_temp():
    temporary = safe_path(Path(tempfile.gettempdir()).resolve())
    if not temporary.is_dir() or any(
        (ancestor / marker).exists()
        for ancestor in (temporary, *temporary.parents)
        for marker in ("catalogue.json", "ledger.sqlite", "warehouse", ".state/ledger.sqlite")
    ):
        raise CurrentError(
            "backup verification temporary directory must be outside catalogue and legacy roots"
        )
    return temporary


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


def _files(settings, conn, *, private=False):
    _validate_existing_pending(settings, conn)
    result = {"catalogue.json", "catalogue.duckdb"}
    if safe_path(settings.root / "config.toml").exists():
        result.add("config.toml")
    if safe_path(settings.published_dir / "current.json").exists():
        if not private:
            verify_current(settings, connection=conn)
        pointer = _pointer(settings)
        result.add("published/current.json")
        for identifier in {pointer["release_id"], pointer.get("previous_release_id")} - {None}:
            if not isinstance(identifier, str) or not _ID.fullmatch(identifier):
                raise CurrentError("unsafe previous release identity")
            release = settings.published_dir / "releases" / identifier
            if private:
                manifest = json.loads(
                    read_regular_bytes(release / "manifest.json", max_bytes=4 * 1024**2)
                )
                current = identifier == pointer["release_id"]
                store_generation = get_state(conn, "generation", 0)
                if (
                    manifest.get("release_id") != identifier
                    or type(manifest.get("generation")) is not int
                    or not 0 <= manifest["generation"] <= store_generation
                ):
                    raise CurrentError("backup publication identity or generation mismatch")
                if current and (
                    _physical(release / "manifest.json")["sha256"] != pointer.get("manifest_sha256")
                ):
                    raise CurrentError("current manifest identity or hash mismatch")
                _verify_release_on_connection(
                    release,
                    conn,
                    bind_store=current
                    and manifest.get("generation") == get_state(conn, "generation", 0),
                )
            else:
                _verify_release_on_connection(release, conn, bind_store=False)
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
        source_path = safe_path(
            source
            / (_relative(name).as_posix() + (".gz" if expected.get("encoding") == "gzip" else ""))
        )
        target = safe_path(destination / name)
        target.parent.mkdir(parents=True, exist_ok=True)
        digest, size = hashlib.sha256(), 0
        try:
            with source_path.open("rb") as stored, target.open("xb") as outgoing:
                incoming = (
                    gzip.GzipFile(fileobj=stored, mode="rb") if expected.get("encoding") else stored
                )
                try:
                    while block := incoming.read(min(1024**2, expected["bytes"] - size + 1)):
                        _deadline(deadline)
                        if size + len(block) > expected["bytes"]:
                            raise CurrentError("backup expanded size exceeds inventory")
                        outgoing.write(block)
                        digest.update(block)
                        size += len(block)
                finally:
                    if incoming is not stored:
                        incoming.close()
                outgoing.flush()
                os.fsync(outgoing.fileno())
        except (gzip.BadGzipFile, EOFError, OSError, zlib.error) as exc:
            raise CurrentError("corrupt compressed backup") from exc
        if {"bytes": size, "sha256": digest.hexdigest()} != {
            key: expected[key] for key in ("bytes", "sha256")
        }:
            raise CurrentError("backup copy changed or is corrupt")
        _fsync_dir(target.parent)


def _compress_database(source, destination, expected, deadline, *, settings):
    target = destination / "catalogue.duckdb.gz"
    digest, size = hashlib.sha256(), 0
    compressor = zlib.compressobj(level=6, wbits=31)
    with source.open("rb") as incoming, target.open("xb", buffering=0) as stored:

        def write(encoded):
            if encoded:
                _deadline(deadline)
                admit(settings, additional_bytes=len(encoded) + 65536)
                if stored.write(encoded) != len(encoded):
                    raise CurrentError("incomplete compressed backup write")

        while block := incoming.read(1024**2):
            _deadline(deadline)
            if size + len(block) > expected["bytes"]:
                raise CurrentError("backup source grew during compression")
            write(compressor.compress(block))
            digest.update(block)
            size += len(block)
        write(compressor.flush())
        os.fsync(stored.fileno())
    if {"bytes": size, "sha256": digest.hexdigest()} != expected:
        raise CurrentError("backup source changed during compression")
    physical = _physical_checked(target, deadline)
    return {
        **expected,
        "encoding": "gzip",
        "stored_bytes": physical["bytes"],
        "stored_sha256": physical["sha256"],
    }


def _validate(settings, deadline, *, evidence_root=None):
    _deadline(deadline)
    with connect_store(settings, read_only=True) as conn:
        result = validate_store(conn)
        evidence = (
            settings
            if evidence_root is None
            else CurrentSettings(evidence_root, limits=settings.limits)
        )
        expected = _files(evidence, conn, private=evidence_root is not None)
    _deadline(deadline)
    return result, expected


def _inventory(path, deadline):
    safe_path(path)
    try:
        value = json.loads(
            read_regular_bytes(path / "backup.json", max_bytes=4 * 1024**2),
            object_pairs_hook=_unique_pairs,
        )
    except (ValueError, TypeError) as exc:
        raise CurrentError("invalid backup manifest") from exc
    if (
        not isinstance(value, dict)
        or set(value) != {"contract", "files"}
        or value.get("contract") != BACKUP_CONTRACT
        or not isinstance(value.get("files"), dict)
    ):
        raise CurrentError("unsupported backup contract")
    inventory = value["files"]
    if not {"catalogue.json", "catalogue.duckdb"}.issubset(inventory):
        raise CurrentError("incomplete backup")
    stored_files = set()
    for name, expected in inventory.items():
        _relative(name)
        fields = {"bytes", "sha256"}
        if name == "catalogue.duckdb":
            fields |= {"encoding", "stored_bytes", "stored_sha256"}
        if not isinstance(expected, dict) or set(expected) != fields:
            raise CurrentError("invalid backup file descriptor")
        if name == "catalogue.duckdb" and expected["encoding"] != "gzip":
            raise CurrentError("unsupported backup encoding")
        for key in ("bytes", "stored_bytes") if name == "catalogue.duckdb" else ("bytes",):
            if type(expected[key]) is not int or expected[key] < 0:
                raise CurrentError("invalid backup byte count")
        for key in ("sha256", "stored_sha256") if name == "catalogue.duckdb" else ("sha256",):
            if (
                not isinstance(expected[key], str)
                or re.fullmatch(r"[0-9a-f]{64}", expected[key]) is None
            ):
                raise CurrentError("invalid backup hash")
        stored_name = name + (".gz" if name == "catalogue.duckdb" else "")
        stored_files.add(stored_name)
        _deadline(deadline)
        try:
            physical = _physical_checked(path / stored_name, deadline)
        except OSError as exc:
            raise CurrentError("missing or unreadable backup member") from exc
        prefix = "stored_" if name == "catalogue.duckdb" else ""
        if physical != {key: expected[prefix + key] for key in ("bytes", "sha256")}:
            raise CurrentError("corrupt backup inventory")
    actual = set()
    allowed_directories = {
        str(parent) for name in stored_files for parent in Path(name).parents if str(parent) != "."
    }
    for child in path.rglob("*"):
        safe_path(child)
        if child.is_file():
            actual.add(str(child.relative_to(path)))
        elif not child.is_dir() or str(child.relative_to(path)) not in allowed_directories:
            raise CurrentError("unexpected backup contents")
    if actual != stored_files | {"backup.json"}:
        raise CurrentError("unexpected backup contents")
    return value


def verify_backup(path, *, deadline=None):
    path = safe_path(Path(path))
    temporary = _verification_temp()
    value = _inventory(path, deadline)
    limits = CurrentSettings(path).limits
    size = sum(
        value["files"][name]["bytes"]
        for name in ("catalogue.duckdb", "catalogue.json")
        if name in value["files"]
    )
    if size >= limits.max_temp_bytes:
        raise CurrentError("backup verification temporary allowance exhausted")
    if sum(item["bytes"] for item in value["files"].values()) > limits.max_retained_bytes:
        raise CurrentError("backup expanded retained allowance exhausted")
    if shutil.disk_usage(temporary).free - limits.max_temp_bytes < limits.free_reserve_bytes:
        raise CurrentError("backup verification free-space reserve exhausted")
    with tempfile.TemporaryDirectory(prefix="catalogue-verify-", dir=temporary) as directory:
        root = Path(directory).resolve() / "root"
        root.mkdir()
        _copy(
            path,
            root,
            {name: value["files"][name] for name in ("catalogue.duckdb", "catalogue.json")},
            deadline,
        )
        result, expected = _validate(
            CurrentSettings(
                root, limits=replace(limits, max_temp_bytes=limits.max_temp_bytes - size)
            ),
            deadline,
            evidence_root=path,
        )
        if set(expected) != set(value["files"]):
            raise CurrentError("backup inventory does not match recovery state")
    if _inventory(path, deadline) != value:
        raise CurrentError("backup changed during verification")
    return {**value, **result, "path": str(path)}


def create_backup(settings, *, deadline=None):
    validate_root(settings.root)
    _verification_temp()
    _deadline(deadline)
    with connect_store(settings) as conn:
        validate_store(conn)
        files = _files(settings, conn)
        conn.execute("CHECKPOINT")
    inventory = {name: _physical_checked(settings.root / name, deadline) for name in files}
    size = sum(item["bytes"] for item in inventory.values())
    private_size = sum(inventory[name]["bytes"] for name in ("catalogue.duckdb", "catalogue.json"))
    if private_size >= settings.limits.max_temp_bytes:
        raise CurrentError("backup verification temporary allowance exhausted")
    admit(settings, additional_bytes=size - inventory["catalogue.duckdb"]["bytes"] + 65536)
    backups = safe_path(settings.backups_dir)
    backups.mkdir(exist_ok=True)
    identifier = datetime.now(UTC).strftime("%Y%m%d") + "-" + uuid4().hex
    stage = backups / (".staging-" + uuid4().hex)
    stage.mkdir()
    try:
        _copy(
            settings.root,
            stage,
            {name: item for name, item in inventory.items() if name != "catalogue.duckdb"},
            deadline,
        )
        compressed = _compress_database(
            settings.store_path, stage, inventory["catalogue.duckdb"], deadline, settings=settings
        )
        if any(
            _physical_checked(settings.root / name, deadline) != expected
            for name, expected in inventory.items()
        ):
            raise CurrentError("backup source changed during copy")
        inventory["catalogue.duckdb"] = compressed
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
        "stored_bytes": directory_bytes(final),
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
    if size > CurrentSettings(destination).limits.max_retained_bytes:
        raise CurrentError("restore expanded retained allowance exhausted")
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
        current = _inventory(path, deadline)
        if current["files"] != verified["files"]:
            raise CurrentError("backup changed during restore")
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
