"""Test helper: run the dbt project against an explicit warehouse path."""

from __future__ import annotations

import subprocess
from pathlib import Path

from oddsfox_catalogue.dbt_runner import DBT_DIR, run_dbt_at


def run_dbt(args: list[str], warehouse: Path, work_dir: Path) -> subprocess.CompletedProcess[str]:
    return run_dbt_at(
        args,
        project_dir=DBT_DIR,
        profiles_dir=DBT_DIR,
        warehouse=warehouse,
        work_dir=work_dir / "dbt",
    )
