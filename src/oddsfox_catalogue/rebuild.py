"""Offline, isolated raw rebuild with complete schema and semantic SHA-256 comparisons."""

from __future__ import annotations

import os
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import rebuild_from_raw
from oddsfox_catalogue.certification import capture_binding
from oddsfox_catalogue.config import PathSettings, Settings
from oddsfox_catalogue.fingerprints import semantic_fingerprint
from oddsfox_catalogue.limits import (
    _union_bytes,
    enforce_storage_limits,
    remaining_temp_bytes,
    retained_bytes,
)
from oddsfox_catalogue.pipeline import dbt_stage, load_stage
from oddsfox_catalogue.semantics import (
    SEMANTIC_RELATIONS,
    bounded_connection,
    published_snapshot,
    semantic_query,
    warehouse_snapshot,
)

VERIFIED_TABLES = SEMANTIC_RELATIONS


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
    scratch = Path(os.path.abspath(scratch))
    if any(path.is_symlink() for path in (scratch, *scratch.parents)):
        raise ValueError("scratch root contains a symlink")
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


def rebuild_and_verify(settings: Settings, *, scratch: Path | None = None) -> RebuildReport:
    """Require complete evidence and rebuild into a new root, never deleting prior work.

    The caller holds the source root lock. Failed scratch evidence is retained for diagnosis.
    """
    enforce_storage_limits(settings)
    source_capture = capture_binding(settings)
    source_warehouse = warehouse_snapshot(settings)
    source_published = published_snapshot(settings)
    remaining = settings.capture.max_retained_bytes - retained_bytes(settings)
    if remaining <= 0:
        raise ValueError("no retained storage allowance remains for raw rebuild")
    scratch = scratch or settings.state_dir / "rebuild" / uuid.uuid4().hex
    scratch = Path(os.path.abspath(scratch))
    if any(path.is_symlink() for path in (scratch, *scratch.parents)):
        raise ValueError("scratch root contains a symlink")
    if scratch.exists():
        raise FileExistsError("raw rebuild requires a fresh scratch root")
    if scratch.is_relative_to(settings.raw_dir):
        raise ValueError("scratch root cannot be inside immutable raw evidence")
    scratch.parent.mkdir(parents=True, exist_ok=True)
    scratch.mkdir()
    rebuilt = scratch_settings(settings, scratch)
    rebuilt = replace(
        rebuilt,
        capture=replace(
            rebuilt.capture,
            max_retained_bytes=remaining,
            max_temp_bytes=remaining_temp_bytes(settings),
        ),
    )
    with Ledger(rebuilt.ledger_path) as ledger:
        rebuild_from_raw(rebuilt, ledger)
    _combined_limits(settings, rebuilt)
    load_stage(rebuilt)
    _combined_limits(settings, rebuilt)
    built = dbt_stage(rebuilt, ["build"])
    if built.returncode != 0:
        return RebuildReport(
            False,
            rebuilt.warehouse_path,
            mismatches=[f"dbt build failed in scratch: {built.stdout[-1000:]}"],
        )
    fresh_warehouse = warehouse_snapshot(rebuilt)
    fresh_published = published_snapshot(rebuilt)
    fresh_capture = capture_binding(rebuilt)
    _combined_limits(settings, rebuilt)
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
