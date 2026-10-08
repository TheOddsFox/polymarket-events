"""Run the dbt project in a subprocess, after the warehouse is released by dlt and DuckDB.

dbt-duckdb holds the DuckDB file for the duration of a build. The probe below proves no
other connection (loader, operator shell, or a crashed run) still holds it, so a build can
never race a load.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

import duckdb

from oddsfox_catalogue.config import Settings

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"


class WarehouseBusy(RuntimeError):
    """The DuckDB file is held by another connection; dbt must not start."""


def assert_warehouse_released(warehouse: Path) -> None:
    """Open and close the warehouse read-write. Fails fast if another process holds it."""
    if not warehouse.exists():
        return
    try:
        connection = duckdb.connect(str(warehouse))
    except duckdb.Error as exc:
        raise WarehouseBusy(f"warehouse {warehouse} is held by another connection: {exc}") from exc
    connection.close()


def run_dbt_at(
    args: Sequence[str],
    *,
    project_dir: Path,
    profiles_dir: Path,
    warehouse: Path,
    work_dir: Path,
) -> subprocess.CompletedProcess[str]:
    """Run ``dbt <args>`` with explicit paths. Target and logs are kept under ``work_dir``."""
    assert_warehouse_released(warehouse)
    work_dir.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "CATALOGUE_WAREHOUSE": str(warehouse)}
    command = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        *args,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--target-path",
        str(work_dir / "target"),
        "--log-path",
        str(work_dir / "logs"),
        "--no-use-colors",
    ]
    return subprocess.run(
        command, env=env, capture_output=True, text=True, check=False, cwd=work_dir
    )


def run_dbt(settings: Settings, args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    """Run dbt for the project's own settings: warehouse, project, and state directory."""
    return run_dbt_at(
        args,
        project_dir=settings.dbt_project_dir,
        profiles_dir=settings.dbt_profiles_dir,
        warehouse=settings.warehouse_path,
        work_dir=settings.state_dir / "dbt",
    )
