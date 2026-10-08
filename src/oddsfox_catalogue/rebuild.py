"""Offline rebuild from raw pages, verified against the live warehouse.

``catalogue rebuild --verify`` proves that the raw pages are enough to reproduce the warehouse.
It rebuilds the ledger from raw manifests, loads every page into a scratch warehouse, runs the
dbt build there, and compares a fingerprint of each table. A mismatch means the warehouse holds
state that raw data does not explain, which must be investigated before the next publish.

Fingerprints are order-independent (count, sum of row hashes, XOR of row hashes), so physical
row order never matters. Load-time metadata is excluded because it records when a load ran,
not what was captured: ``_dlt_*`` columns, ``loaded_at``, ``load_id``, ``built_through``, and
``batch_loaded_at``.
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb

from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import rebuild_from_raw
from oddsfox_catalogue.config import Settings, load_settings
from oddsfox_catalogue.pipeline import dbt_stage, load_stage

VERIFIED_TABLES: tuple[str, ...] = (
    "bronze.event_observations",
    "bronze.market_observations",
    "core.events_current",
    "core.markets_current",
    "core.outcomes_current",
    "core.event_tags_current",
    "core.event_series_current",
    "core.market_tags_current",
    "core.market_event_bridge",
    "history.event_history",
    "history.market_history",
    "marts.mart_event_catalogue",
)

EXCLUDED_COLUMNS = frozenset({"loaded_at", "load_id", "built_through", "batch_loaded_at"})
EXCLUDED_PREFIXES = ("_dlt",)


@dataclass
class RebuildReport:
    matched: bool
    rebuilt_to: Path
    tables: dict[str, dict[str, Any]] = field(default_factory=dict)
    mismatches: list[str] = field(default_factory=list)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _included_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        [schema, table],
    ).fetchall()
    return [
        name
        for (name,) in rows
        if name not in EXCLUDED_COLUMNS and not name.startswith(EXCLUDED_PREFIXES)
    ]


def fingerprint_table(con: duckdb.DuckDBPyConnection, qualified: str) -> dict[str, Any] | None:
    """Fingerprint one table, or return None if it does not exist."""
    schema, table = qualified.split(".", 1)
    columns = _included_columns(con, schema, table)
    if not columns:
        return None
    select_list = ", ".join(_quote(c) for c in columns)
    row = con.execute(
        f"SELECT count(*), sum(hash(r)), bit_xor(hash(r)) FROM "
        f"(SELECT {select_list} FROM {_quote(schema)}.{_quote(table)}) AS r"
    ).fetchone()
    assert row is not None
    rows, total, xor = row
    return {"rows": int(rows), "sum_hash": int(total or 0), "xor_hash": int(xor or 0)}


def warehouse_fingerprints(warehouse: Path, tables: Sequence[str]) -> dict[str, dict[str, Any]]:
    if not warehouse.exists():
        return {}
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        result: dict[str, dict[str, Any]] = {}
        for qualified in tables:
            fingerprint = fingerprint_table(con, qualified)
            if fingerprint is not None:
                result[qualified] = fingerprint
        return result
    finally:
        con.close()


def scratch_settings(settings: Settings, scratch: Path) -> Settings:
    """Same project and raw data, but a fresh state directory and warehouse under ``scratch``."""
    scratch = scratch.resolve()
    env = {
        **os.environ,
        "CATALOGUE_PATHS_STATE_DIR": str(scratch / "state"),
        "CATALOGUE_PATHS_WAREHOUSE_FILE": str(scratch / "catalogue.duckdb"),
        # The project and raw data are shared with the live settings, not copied.
        "CATALOGUE_PATHS_DATA_DIR": str(settings.data_dir),
        "CATALOGUE_PATHS_DBT_PROJECT_DIR": str(settings.dbt_project_dir),
        "CATALOGUE_PATHS_DBT_PROFILES_DIR": str(settings.dbt_profiles_dir),
    }
    return load_settings(root=settings.root, env=env)


def rebuild_and_verify(settings: Settings, *, scratch: Path | None = None) -> RebuildReport:
    """Rebuild the warehouse from raw pages in a scratch area and compare every verified table."""
    scratch = scratch or settings.state_dir / "rebuild"
    if scratch.exists():
        shutil.rmtree(scratch)
    scratch.mkdir(parents=True)
    rebuilt = scratch_settings(settings, scratch)

    ledger = Ledger(rebuilt.ledger_path)
    try:
        rebuild_from_raw(rebuilt, ledger)
    finally:
        ledger.close()
    load_stage(rebuilt)
    built = dbt_stage(rebuilt, ["build"])
    if built.returncode != 0:
        return RebuildReport(
            matched=False,
            rebuilt_to=rebuilt.warehouse_path,
            mismatches=[f"dbt build failed in scratch: {built.stdout[-1000:]}"],
        )

    live = warehouse_fingerprints(settings.warehouse_path, VERIFIED_TABLES)
    fresh = warehouse_fingerprints(rebuilt.warehouse_path, VERIFIED_TABLES)
    report = RebuildReport(matched=True, rebuilt_to=rebuilt.warehouse_path)
    for table in VERIFIED_TABLES:
        report.tables[table] = {"live": live.get(table), "rebuilt": fresh.get(table)}
        if live.get(table) != fresh.get(table):
            report.matched = False
            report.mismatches.append(f"{table}: live={live.get(table)} rebuilt={fresh.get(table)}")
    return report
