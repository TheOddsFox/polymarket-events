"""Run the dbt project in a subprocess, after the warehouse is released by dlt and DuckDB.

dbt-duckdb holds the DuckDB file for the duration of a build. The probe below proves no
other connection (loader, operator shell, or a crashed run) still holds it, so a build can
never race a load.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from collections.abc import Sequence
from pathlib import Path

import duckdb

from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.limits import remaining_temp_bytes
from oddsfox_catalogue.resources import dbt_project_dir

DBT_DIR = dbt_project_dir()


def is_selected_dbt_command(args: Sequence[str]) -> bool:
    flags = ("--select", "--models", "-s", "-m", "--exclude", "--selector")
    return any(
        argument == flag
        or argument.startswith(flag + "=")
        or (len(flag) == 2 and argument.startswith(flag) and len(argument) > 2)
        for argument in args
        for flag in flags
    )


class WarehouseBusy(RuntimeError):
    """The DuckDB file is held by another connection; dbt must not start."""


def _dbt_environment(
    warehouse: Path, memory_limit: str, temporary_dir: Path, max_temp_bytes: int, threads: int
) -> dict[str, str]:
    return {
        "CATALOGUE_WAREHOUSE": str(warehouse),
        "CATALOGUE_DUCKDB_MEMORY_LIMIT": memory_limit,
        "CATALOGUE_DUCKDB_TEMP_DIRECTORY": str(temporary_dir),
        "CATALOGUE_DUCKDB_MAX_TEMP_BYTES": str(max_temp_bytes),
        "CATALOGUE_DUCKDB_THREADS": str(threads),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }


def dbt_environment(settings: Settings) -> dict[str, str]:
    """The same resource limits for CLI and Dagster dbt subprocesses."""
    return _dbt_environment(
        settings.warehouse_path,
        settings.load.duckdb_memory_limit,
        settings.temporary_dir,
        remaining_temp_bytes(settings),
        settings.load.duckdb_threads,
    )


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
    memory_limit: str = "2GB",
    max_temp_bytes: int = 8 * 1024**3,
    threads: int = 1,
    temporary_dir: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run ``dbt <args>`` with explicit paths. Target and logs are kept under ``work_dir``."""
    assert_warehouse_released(warehouse)
    work_dir.mkdir(parents=True, exist_ok=True)
    temporary_dir = temporary_dir or work_dir / "tmp"
    temporary_dir.mkdir(parents=True, exist_ok=True)
    env = {
        **os.environ,
        **_dbt_environment(warehouse, memory_limit, temporary_dir, max_temp_bytes, threads),
    }
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
        "--no-send-anonymous-usage-stats",
    ]
    return subprocess.run(
        command, env=env, capture_output=True, text=True, check=False, cwd=work_dir
    )


def quality_dbt_vars(settings: Settings) -> dict[str, float]:
    """Publication compares valid releases; dbt has no implicit legacy quality variables."""
    return {}


def with_quality_vars(settings: Settings, args: Sequence[str]) -> list[str]:
    """Return ``args`` with the configured quality vars merged into ``--vars``.

    A ``--vars`` the caller passed (JSON object) wins for the keys it sets. Keys it does not
    set take the configured value.
    """
    defaults = quality_dbt_vars(settings)
    caller, out = dbt_vars(args)
    return [*out, "--vars", json.dumps({**defaults, **caller}, sort_keys=True, allow_nan=False)]


def dbt_vars(args: Sequence[str]) -> tuple[dict, list[str]]:
    """Accept one JSON vars object, with split or equals spelling, and remove its flag."""
    out, values, index = [], [], 0
    while index < len(args):
        argument = args[index]
        if argument == "--vars":
            index += 1
            if index >= len(args):
                raise ValueError("--vars requires a JSON object value")
            values.append(args[index])
        elif argument.startswith("--vars="):
            values.append(argument.split("=", 1)[1])
        else:
            out.append(argument)
        index += 1
    if len(values) > 1:
        raise ValueError("duplicate --vars arguments are ambiguous")
    value = json.loads(values[0]) if values else {}
    if not isinstance(value, dict):
        raise ValueError("--vars must be a JSON object")
    json.dumps(value, allow_nan=False)
    return value, out


def run_dbt(settings: Settings, args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    """Run dbt for the project's own settings: warehouse, project, and state directory."""
    work_dir = settings.state_dir / "dbt" / "invocations" / uuid.uuid4().hex
    if args and args[0] == "build":
        from oddsfox_catalogue.certification import prepare_dbt_execution

        prepare_dbt_execution(settings, work_dir / "target", args=args)
    elif args and args[0] != "parse":
        from oddsfox_catalogue.certification import mark_dirty

        mark_dirty(settings, "dbt execution")
    result = run_dbt_at(
        args,
        project_dir=settings.dbt_project_dir,
        profiles_dir=settings.dbt_profiles_dir,
        warehouse=settings.warehouse_path,
        work_dir=work_dir,
        memory_limit=settings.load.duckdb_memory_limit,
        max_temp_bytes=remaining_temp_bytes(settings),
        threads=settings.load.duckdb_threads,
        temporary_dir=settings.temporary_dir,
    )
    result.target_path = work_dir / "target"
    return result
