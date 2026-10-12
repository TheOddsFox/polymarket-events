"""Offline, isolated raw rebuild with complete schema and semantic SHA-256 comparisons."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field, fields, replace
from importlib.metadata import distributions
from pathlib import Path
from typing import Any

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import rebuild_from_raw
from oddsfox_catalogue.capture.writer import atomic_write_bytes, read_regular_bytes
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
from oddsfox_catalogue.warehouse_version import WAREHOUSE_CONTRACT

VERIFIED_TABLES = SEMANTIC_RELATIONS
REBUILD_RECORD = "rebuild.json"
REBUILD_RECORD_LIMIT = 64 * 1024**2


@dataclass
class RebuildReport:
    matched: bool
    rebuilt_to: Path
    tables: dict[str, dict[str, Any]] = field(default_factory=dict)
    mismatches: list[str] = field(default_factory=list)


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


def _source_binding(settings):
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
        "release": current_release(settings),
        "capture": capture_binding(settings),
        "warehouse": warehouse_snapshot(settings),
        "published": published_snapshot(settings),
    }


def _write_record(scratch, binding, phase):
    record = {"revision": 1, "binding": binding, "phase": phase}
    record["sha256"] = json_descriptor(record)["sha256"]
    body = (
        json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()
    if len(body) > REBUILD_RECORD_LIMIT:
        raise ValueError("rebuild record exceeds byte allowance")
    atomic_write_bytes(scratch / REBUILD_RECORD, body)


def _read_record(scratch, binding):
    record = json.loads(
        read_regular_bytes(
            scratch / REBUILD_RECORD, max_bytes=REBUILD_RECORD_LIMIT, trusted_root=scratch
        )
    )
    if not isinstance(record, dict) or set(record) != {"revision", "binding", "phase", "sha256"}:
        raise ValueError("invalid rebuild record")
    digest = record.pop("sha256")
    if type(record["revision"]) is not int or record["revision"] != 1:
        raise ValueError("unsupported rebuild record revision")
    if json_descriptor(record)["sha256"] != digest:
        raise ValueError("rebuild record checksum mismatch")
    if record["binding"] != binding:
        raise ValueError("rebuild source or runtime binding changed")
    if record["phase"] not in {"initialization", "load", "build", "comparison", "complete"}:
        raise ValueError("invalid rebuild phase")
    return record["phase"]


def rebuild_and_verify(
    settings: Settings, *, scratch: Path | None = None, resume: bool = False
) -> RebuildReport:
    """Require complete evidence and rebuild into a new root, never deleting prior work.

    The caller holds the source root lock. Failed scratch evidence is retained for diagnosis.
    """
    enforce_storage_limits(settings)
    if resume and scratch is None:
        raise ValueError("rebuild resume requires an explicit scratch root")
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
    binding = _source_binding(settings)
    if resume:
        # Reject unrelated/changed scratch before the lock can create any state files there.
        _read_record(scratch, binding)
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
        if resume:
            phase = _read_record(scratch, binding)
        else:
            _write_record(scratch, binding, phase)
        if phase == "initialization":
            if rebuilt.warehouse_path.exists():
                raise ValueError("initializing scratch already contains a warehouse")
            with Ledger(rebuilt.ledger_path) as ledger:
                if ledger._one("SELECT 1 FROM pages WHERE loaded_at IS NOT NULL LIMIT 1"):
                    raise ValueError("initializing scratch already contains load checkpoints")
                rebuild_from_raw(rebuilt, ledger)
            _write_record(scratch, binding, "load")
            phase = "load"
        if phase == "load":
            load_stage(rebuilt)
            _combined_limits(settings, rebuilt)
            _write_record(scratch, binding, "build")
            phase = "build"
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
            _write_record(scratch, binding, "comparison")
        receipt = assert_build_valid(rebuilt)
        fresh_warehouse = receipt["binding"]["warehouse"]
        fresh_published = receipt["binding"]["published"]
        fresh_capture = receipt["binding"]["capture"]
        _combined_limits(settings, rebuilt)
        report = _compare(rebuilt, binding, fresh_warehouse, fresh_published, fresh_capture)
        if report.matched:
            _write_record(scratch, binding, "complete")
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
