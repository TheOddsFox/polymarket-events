"""Offline, isolated raw rebuild with complete schema and semantic SHA-256 comparisons."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import tempfile
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field, fields, replace
from importlib.metadata import distributions
from pathlib import Path
from typing import Any

from oddsfox_catalogue.backup import (
    _inventory,
    _safe_path,
    _snapshot_ledger,
    _stream_file,
    _unique_object,
)
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import rebuild_from_raw
from oddsfox_catalogue.capture.writer import _fsync_dir, atomic_write_bytes, read_regular_bytes
from oddsfox_catalogue.certification import (
    _model_revision,
    assert_build_valid,
    capture_binding,
    read_build_validity,
)
from oddsfox_catalogue.config import PathSettings, Settings
from oddsfox_catalogue.contract import CONTRACT, NORMALIZATION_REVISION
from oddsfox_catalogue.fingerprints import semantic_fingerprint
from oddsfox_catalogue.limits import (
    _union_bytes,
    directory_bytes,
    enforce_storage_limits,
    remaining_temp_bytes,
    retained_bytes,
    temporary_bytes,
)
from oddsfox_catalogue.pipeline import dbt_stage, load_stage
from oddsfox_catalogue.publish import current_release
from oddsfox_catalogue.runlock import run_lock
from oddsfox_catalogue.semantics import (
    SEMANTIC_RELATIONS,
    bounded_connection,
    json_descriptor,
    published_snapshot,
    semantic_query,
    warehouse_snapshot,
)
from oddsfox_catalogue.timing import phase_timing
from oddsfox_catalogue.warehouse_version import WAREHOUSE_CONTRACT

VERIFIED_TABLES = SEMANTIC_RELATIONS
REBUILD_RECORD = "rebuild.json"
REBUILD_RECORD_LIMIT = 64 * 1024**2
REGISTRATIONS = "source-bindings"


@dataclass
class RebuildReport:
    matched: bool
    rebuilt_to: Path
    tables: dict[str, dict[str, Any]] = field(default_factory=dict)
    mismatches: list[str] = field(default_factory=list)
    pending: bool = False


@dataclass(frozen=True)
class _ScratchSettings(Settings):
    raw_source: Path = Path(".")

    @property
    def raw_dir(self) -> Path:
        return self.raw_source


def fingerprint_table(connection, qualified: str):
    """Fingerprint semantic columns with their ordered schema, preserving duplicates."""
    schema, table = qualified.split(".", 1)
    exists = connection.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    if exists is None:
        return None
    return semantic_fingerprint(connection, semantic_query(connection, qualified))


def warehouse_fingerprints(warehouse: Path, tables: Sequence[str]):
    """Focused relation comparisons retain the same finite query defaults as the CLI."""
    if not warehouse.exists():
        return {}
    settings = Settings(warehouse.parent.parent.parent)
    with bounded_connection(settings, path=warehouse) as connection:
        return {
            table: result
            for table in tables
            if (result := fingerprint_table(connection, table)) is not None
        }


def scratch_settings(settings: Settings, scratch: Path) -> Settings:
    """Isolate all writes and locks; only verified raw inputs are shared read-only."""
    if any(path.is_symlink() for path in (scratch, *scratch.parents)):
        raise ValueError("scratch root contains a symlink")
    scratch = Path(os.path.abspath(scratch))
    values = {item.name: getattr(settings, item.name) for item in fields(Settings)}
    values.update(
        root=scratch,
        paths=PathSettings(
            dbt_project_dir=str(settings.dbt_project_dir),
            dbt_profiles_dir=str(settings.dbt_profiles_dir),
        ),
    )
    return _ScratchSettings(**values, raw_source=settings.raw_dir)


def _combined_limits(settings: Settings, rebuilt: Settings):
    paths = (
        settings.root,
        settings.data_dir,
        settings.state_dir,
        settings.dlt_pipelines_dir,
        settings.warehouse_path,
        settings.warehouse_path.with_name(settings.warehouse_path.name + ".wal"),
        rebuilt.root,
    )
    if _union_bytes(paths) > settings.capture.max_retained_bytes:
        raise ValueError("raw rebuild exceeds combined retained storage allowance")
    if (
        _union_bytes(
            (
                settings.temporary_dir,
                settings.dlt_pipelines_dir,
                rebuilt.temporary_dir,
                rebuilt.dlt_pipelines_dir,
            )
        )
        > settings.capture.max_temp_bytes
    ):
        raise ValueError("raw rebuild exceeds combined temporary storage allowance")


def _runtime_revision():
    package = Path(__file__).parent
    members, total = {}, 0
    for path in sorted(package.rglob("*")):
        relative = path.relative_to(package)
        if any(
            part in {"target", "logs", "dbt_packages", "__pycache__"} for part in relative.parts
        ):
            continue
        if not path.is_file() or path.suffix not in {".py", ".sql", ".yml", ".yaml"}:
            continue
        body = read_regular_bytes(path, max_bytes=REBUILD_RECORD_LIMIT, trusted_root=package)
        total += len(body)
        if total > REBUILD_RECORD_LIMIT:
            raise ValueError("rebuild runtime exceeds source size allowance")
        members[relative.as_posix()] = hashlib.sha256(body).hexdigest()
    return json_descriptor(
        {
            "members": members,
            "python": platform.python_version(),
            "dependencies": sorted((d.metadata["Name"], d.version) for d in distributions()),
        }
    )


def _source_configuration(settings):
    effective_vars = {}
    receipt = read_build_validity(settings)
    if receipt is not None and receipt["status"] == "valid":
        payload = receipt["payload_json"]
        if (
            not isinstance(payload, str)
            or len(payload.encode()) > REBUILD_RECORD_LIMIT
            or hashlib.sha256(payload.encode()).hexdigest() != receipt["payload_sha256"]
        ):
            raise ValueError("invalid source build receipt")
        effective_vars = json.loads(payload)["binding"]["effective_vars"]
        if not isinstance(effective_vars, dict):
            raise ValueError("invalid source effective dbt variables")
    return {
        "source": {
            name: str(Path(os.path.abspath(getattr(settings, name))))
            for name in ("root", "raw_dir", "state_dir", "warehouse_path", "published_dir")
        },
        "contract": CONTRACT,
        "warehouse_contract": WAREHOUSE_CONTRACT,
        "normalization_revision": NORMALIZATION_REVISION,
        "runtime": _runtime_revision(),
        "model_revision": _model_revision(settings),
        "effective_vars": effective_vars,
        "configuration": {
            name: asdict(getattr(settings, name))
            for name in ("quality", "gamma", "load", "capture")
        },
    }


def _source_binding(settings, *, query_settings=None):
    query_settings = query_settings or settings
    return {
        **_source_configuration(settings),
        "release": current_release(query_settings),
        "capture": capture_binding(query_settings),
        "warehouse": warehouse_snapshot(query_settings),
        "published": published_snapshot(query_settings),
    }


@phase_timing("recovery.source_seal")
def _source_seal(settings, *, query_settings=None):
    """Exact evidence bytes plus a consistent SQLite snapshot, not a lock-based cache."""
    query_settings = query_settings or settings
    seal = {"context": _source_configuration(settings)}
    for name, root in (("raw", settings.raw_dir), ("published", settings.published_dir)):
        _safe_path(root)
        if not root.exists():
            seal[name] = None
            continue
        files, directories = _inventory(root)
        seal[name] = json_descriptor(
            {
                "directories": directories,
                "files": {
                    name: _stream_file(root / name, max_bytes=size)
                    for name, size in sorted(files.items())
                },
            }
        )
        if _inventory(root) != (files, directories):
            raise ValueError("rebuild source inventory changed during sealing")
    for name, path in (
        ("warehouse", settings.warehouse_path),
        ("warehouse_wal", settings.warehouse_path.with_name(settings.warehouse_path.name + ".wal")),
    ):
        _safe_path(path)
        seal[name] = (
            _stream_file(path, max_bytes=settings.capture.max_retained_bytes)
            if path.exists()
            else None
        )
    if seal["warehouse"] is None:
        raise ValueError("rebuild source warehouse is missing")
    _safe_path(settings.ledger_path)
    estimate = settings.ledger_path.stat().st_size
    wal = settings.ledger_path.with_name(settings.ledger_path.name + "-wal")
    _safe_path(wal)
    if wal.exists():
        estimate += wal.stat().st_size
    if estimate <= 0 or estimate > remaining_temp_bytes(query_settings):
        raise ValueError("ledger snapshot exceeds temporary allowance")
    enforce_storage_limits(query_settings, additional_bytes=estimate)
    query_settings.temporary_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="rebuild-ledger-", dir=query_settings.temporary_dir
    ) as d:
        snapshot = Path(d) / "ledger.sqlite"
        _snapshot_ledger(settings.ledger_path, snapshot, max_bytes=estimate)
        enforce_storage_limits(query_settings)
        seal["ledger"] = _stream_file(snapshot, max_bytes=estimate)
    return seal


def _source_query_settings(settings, rebuilt):
    source_temporary = temporary_bytes(settings)
    combined_temporary = _union_bytes(
        (
            settings.temporary_dir,
            settings.dlt_pipelines_dir,
            rebuilt.temporary_dir,
            rebuilt.dlt_pipelines_dir,
        )
    )
    source_retained = retained_bytes(settings)
    combined_retained = _union_bytes(
        (
            settings.root,
            settings.data_dir,
            settings.state_dir,
            settings.dlt_pipelines_dir,
            settings.warehouse_path,
            settings.warehouse_path.with_name(settings.warehouse_path.name + ".wal"),
            rebuilt.root,
        )
    )
    return replace(
        settings,
        capture=replace(
            settings.capture,
            max_temp_bytes=settings.capture.max_temp_bytes - combined_temporary + source_temporary,
            max_retained_bytes=settings.capture.max_retained_bytes
            - combined_retained
            + source_retained,
        ),
    )


def _registration_path(settings, scratch):
    identity = hashlib.sha256(str(scratch).encode()).hexdigest()
    return settings.state_dir / "rebuild" / REGISTRATIONS / f"{identity}.json", identity


def _write_registration(settings, scratch, binding, seal):
    path, identity = _registration_path(settings, scratch)
    _safe_path(path)
    value = {
        "revision": 1,
        "identity": identity,
        "scratch": str(scratch),
        "binding": binding,
        "seal": seal,
    }
    body = (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()
    if len(body) > REBUILD_RECORD_LIMIT:
        raise ValueError("rebuild source registration exceeds byte allowance")
    enforce_storage_limits(settings, additional_bytes=len(body))
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(body)
        handle.flush()
        os.fsync(handle.fileno())
    _fsync_dir(path.parent)
    return {"identity": identity, "sha256": hashlib.sha256(body).hexdigest()}


def _write_record(scratch, registration, phase):
    record = {"revision": 2, "registration": registration, "phase": phase}
    record["sha256"] = json_descriptor(record)["sha256"]
    body = (
        json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()
    if len(body) > REBUILD_RECORD_LIMIT:
        raise ValueError("rebuild record exceeds byte allowance")
    atomic_write_bytes(scratch / REBUILD_RECORD, body)


def _read_record(scratch):
    record = json.loads(
        read_regular_bytes(
            scratch / REBUILD_RECORD, max_bytes=REBUILD_RECORD_LIMIT, trusted_root=scratch
        ),
        object_pairs_hook=_unique_object,
    )
    if not isinstance(record, dict) or set(record) != {
        "revision",
        "registration",
        "phase",
        "sha256",
    }:
        raise ValueError("invalid rebuild record")
    digest = record.pop("sha256")
    if type(record["revision"]) is not int or record["revision"] != 2:
        raise ValueError("unsupported rebuild record revision; use fresh scratch")
    if json_descriptor(record)["sha256"] != digest:
        raise ValueError("rebuild record checksum mismatch")
    if not isinstance(record["phase"], str) or record["phase"] not in {
        "initialization",
        "load",
        "build",
        "comparison",
        "complete",
    }:
        raise ValueError("invalid rebuild phase")
    registration = record["registration"]
    if (
        not isinstance(registration, dict)
        or set(registration) != {"identity", "sha256"}
        or any(
            not isinstance(v, str) or not re.fullmatch(r"[0-9a-f]{64}", v)
            for v in registration.values()
        )
    ):
        raise ValueError("invalid rebuild source registration reference")
    return record


def _validated_rebuild_source(settings, scratch, *, query_settings=None):
    query_settings = query_settings or _source_query_settings(
        settings, scratch_settings(settings, scratch)
    )
    record = _read_record(scratch)
    path, identity = _registration_path(settings, scratch)
    reference = record["registration"]
    if reference["identity"] != identity:
        raise ValueError("rebuild source registration belongs to another scratch")
    body = read_regular_bytes(path, max_bytes=REBUILD_RECORD_LIMIT, trusted_root=settings.state_dir)
    if hashlib.sha256(body).hexdigest() != reference["sha256"]:
        raise ValueError("rebuild source registration checksum mismatch")
    value = json.loads(body, object_pairs_hook=_unique_object)
    if (
        not isinstance(value, dict)
        or set(value) != {"revision", "identity", "scratch", "binding", "seal"}
        or type(value["revision"]) is not int
        or value["revision"] != 1
        or value["identity"] != identity
        or value["scratch"] != str(scratch)
        or not isinstance(value["binding"], dict)
        or not isinstance(value["seal"], dict)
    ):
        raise ValueError("invalid rebuild source registration")
    if _source_seal(settings, query_settings=query_settings) != value["seal"]:
        raise ValueError("rebuild source or runtime binding changed")
    return value["binding"], record["phase"], reference


@phase_timing("recovery.rebuild_and_verify")
def rebuild_and_verify(
    settings: Settings, *, scratch: Path | None = None, resume: bool = False, step: bool = False
) -> RebuildReport:
    """Require complete evidence and rebuild into a new root, never deleting prior work.

    The caller holds the source root lock. Failed scratch evidence is retained for diagnosis.
    """
    enforce_storage_limits(settings)
    if resume and scratch is None:
        raise ValueError("rebuild resume requires an explicit scratch root")
    if step and scratch is None:
        raise ValueError("rebuild step requires an explicit scratch root")
    scratch = scratch or settings.state_dir / "rebuild" / uuid.uuid4().hex
    if any(path.is_symlink() for path in (scratch, *scratch.parents)):
        raise ValueError("scratch root contains a symlink")
    scratch = Path(os.path.abspath(scratch))
    if scratch.exists() and not resume:
        raise FileExistsError("raw rebuild requires a fresh scratch root")
    allowed_nested = Path(os.path.abspath(settings.state_dir / "rebuild"))
    protected = [
        Path(os.path.abspath(path))
        for path in (
            settings.root,
            settings.raw_dir,
            settings.data_dir,
            settings.state_dir,
            settings.warehouse_path,
            settings.published_dir,
            settings.dlt_pipelines_dir,
            settings.state_dir / "rebuild" / REGISTRATIONS,
        )
    ]
    if any(path.is_relative_to(scratch) for path in protected) or any(
        scratch.is_relative_to(path)
        and not (
            path == Path(os.path.abspath(settings.root))
            or (
                path == Path(os.path.abspath(settings.state_dir))
                and scratch.is_relative_to(allowed_nested)
                and scratch != allowed_nested
            )
        )
        for path in protected
    ):
        raise ValueError("scratch root overlaps protected source evidence")
    if resume and not scratch.is_dir():
        raise ValueError("rebuild resume requires existing bound scratch")
    rebuilt = scratch_settings(settings, scratch)
    _combined_limits(settings, rebuilt)
    # Scratch bytes constrain source reads, but do not change the persisted operation policy.
    query_settings = _source_query_settings(settings, rebuilt)
    if resume:
        # Reject unrelated/changed scratch before the lock can create any state files there.
        binding, phase, registration = _validated_rebuild_source(
            settings, scratch, query_settings=query_settings
        )
    else:
        registration_path, _ = _registration_path(settings, scratch)
        if registration_path.exists() or registration_path.is_symlink():
            raise FileExistsError("rebuild source registration exists; use a fresh scratch path")
        seal = _source_seal(settings, query_settings=query_settings)
        binding = _source_binding(settings, query_settings=query_settings)
        if _source_seal(settings, query_settings=query_settings) != seal:
            raise ValueError("rebuild source changed during semantic validation")
        registration = _write_registration(settings, scratch, binding, seal)
        phase = "initialization"
    scratch.parent.mkdir(parents=True, exist_ok=True)
    if not resume:
        scratch.mkdir()
    rebuilt = scratch_settings(settings, scratch)
    _combined_limits(settings, rebuilt)
    combined = _union_bytes(
        (
            *protected,
            settings.warehouse_path.with_name(settings.warehouse_path.name + ".wal"),
            rebuilt.root,
        )
    )
    remaining = settings.capture.max_retained_bytes - combined
    temp_used = _union_bytes(
        (
            settings.temporary_dir,
            settings.dlt_pipelines_dir,
            rebuilt.temporary_dir,
            rebuilt.dlt_pipelines_dir,
        )
    )
    rebuilt = replace(
        rebuilt,
        capture=replace(
            rebuilt.capture,
            max_retained_bytes=directory_bytes(scratch) + remaining,
            max_temp_bytes=temporary_bytes(rebuilt) + settings.capture.max_temp_bytes - temp_used,
        ),
    )
    with run_lock(rebuilt.run_lock_path):
        if not resume:
            _write_record(scratch, registration, phase)
            if step:
                return RebuildReport(False, rebuilt.warehouse_path, pending=True)

        def checkpoint(next_phase):
            _validated_rebuild_source(settings, scratch)
            _write_record(scratch, registration, next_phase)

        if phase == "initialization":
            if rebuilt.warehouse_path.exists():
                raise ValueError("initializing scratch already contains a warehouse")
            with Ledger(rebuilt.ledger_path) as ledger:
                if ledger._one("SELECT 1 FROM pages WHERE loaded_at IS NOT NULL LIMIT 1"):
                    raise ValueError("initializing scratch already contains load checkpoints")
                rebuild_from_raw(rebuilt, ledger)
            checkpoint("load")
            phase = "load"
            if step:
                return RebuildReport(False, rebuilt.warehouse_path, pending=True)
        if phase == "load":
            load_stage(rebuilt)
            _combined_limits(settings, rebuilt)
            checkpoint("build")
            phase = "build"
            if step:
                return RebuildReport(False, rebuilt.warehouse_path, pending=True)
        if phase == "build":
            arguments = ["build"]
            if binding.get("effective_vars"):
                arguments += ["--vars", json.dumps(binding["effective_vars"], sort_keys=True)]
            built = dbt_stage(rebuilt, arguments)
            if built.returncode != 0:
                return RebuildReport(
                    False,
                    rebuilt.warehouse_path,
                    mismatches=[f"dbt build failed in scratch: {built.stdout[-1000:]}"],
                )
            checkpoint("comparison")
            if step:
                return RebuildReport(False, rebuilt.warehouse_path, pending=True)
        receipt = assert_build_valid(rebuilt)
        fresh_warehouse = receipt["binding"]["warehouse"]
        fresh_published = receipt["binding"]["published"]
        fresh_capture = receipt["binding"]["capture"]
        _combined_limits(settings, rebuilt)
        report = _compare(rebuilt, binding, fresh_warehouse, fresh_published, fresh_capture)
        if report.matched:
            checkpoint("complete")
        _combined_limits(settings, rebuilt)
        return report


def _compare(rebuilt, binding, fresh_warehouse, fresh_published, fresh_capture):
    source_warehouse, source_published, source_capture = (
        binding["warehouse"],
        binding["published"],
        binding["capture"],
    )
    report = RebuildReport(True, rebuilt.warehouse_path)
    comparisons = {
        **{
            name: (source_warehouse["relations"][name], fresh_warehouse["relations"][name])
            for name in VERIFIED_TABLES
        },
        **{
            f"schema:{name}": (schema, fresh_warehouse["schemas"].get(name))
            for name, schema in source_warehouse["schemas"].items()
        },
        **{
            f"published:{name}": (fingerprint, fresh_published.get(name))
            for name, fingerprint in source_published.items()
        },
        "coverage": (source_capture["coverage"], fresh_capture["coverage"]),
        "capture_inventory": (source_capture["inventory"], fresh_capture["inventory"]),
    }
    for name, (live, fresh) in comparisons.items():
        report.tables[name] = {"live": live, "rebuilt": fresh}
        if live != fresh:
            report.matched = False
            report.mismatches.append(f"{name}: semantic content or schema differs")
    return report
